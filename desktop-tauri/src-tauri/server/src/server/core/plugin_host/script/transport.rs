mod lifecycle;
mod pending;
mod frozen;
pub(in crate::server::core::plugin_host) use lifecycle::{HttpDeadline, HttpHandle};

use std::sync::Arc;
use std::time::Duration;

use agent2api_provider_protocol as proto;
use base64::Engine;
use serde_json::{json, Value};

use crate::server::core::{egress, proxies::{ResolvedProxy, ProxyResolution, resolve_account_proxy}};
use lifecycle::Download;
use crate::server::core::plugin_host::wasm::conversation::WorkBuddyExecution;

fn error(message: impl Into<String>) -> proto::WireError {
    proto::WireError::with_code(message, proto::WireError::INTERNAL)
}

fn cancellation() -> proto::WireError {
    proto::WireError::with_code("脚本上游请求已取消", proto::WireError::ABORTED)
}

/// 上游限流（429）的退避重试次数。
///
/// ── 为什么需要它 ─────────────────────────────────────────────
/// 边缘节点（阿里云 ESA 这类 WAF）按 IP + 频率限流，命中时返回 **429 +
/// `Retry-After` + 空响应体**。插件侧把「空体」一律当成「上游响应不是
/// JSON」，于是限流被报成协议错误 —— 用户看到的是「套餐探测：上游响应
/// 不是 JSON」，完全指不到真实原因。
///
/// 插件一次业务动作会并发打好几个请求到同一个域名（探测 / 营销 / 上报 /
/// 目录 / 余额各一条），**自己就能把自己撞进限流**。所以退避重试放在出网
/// 这一层最合适：等上游给的秒数再试，通常就能过。
const RATE_LIMIT_RETRIES: usize = 2;

/// 单次退避的上限。`Retry-After` 可能给几十秒，插件侧等不了那么久。
const RATE_LIMIT_BACKOFF_MAX: Duration = Duration::from_secs(3);

/// 读上游给的 `Retry-After`（秒数）。缺失或非法按 1 秒算。
fn retry_after(headers: &reqwest::header::HeaderMap) -> Duration {
    let seconds = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(1)
        .clamp(1, 30);
    Duration::from_secs(seconds).min(RATE_LIMIT_BACKOFF_MAX)
}

/// 退避抖动（0–400 毫秒）。
///
/// 并发的几条请求会同时撞限流、同时退避、同时重试 —— 不加抖动就还是同一
/// 拍再撞一次。抖动把这个「同拍」打散。
fn backoff_jitter() -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.subsec_nanos())
        .unwrap_or(0);
    Duration::from_millis(u64::from(nanos % 400))
}

/// 插件运行时出网用的候选地址。
///
/// ── 为什么要换地址 ────────────────────────────────────────────
/// 宿主自己的仓库文件读取早就走 jsDelivr 了，但**插件在运行时自己拼出来的
/// 地址**（模型目录、官方配置、索引文件……）仍然直打 `raw.githubusercontent.com`。
/// 该域在部分网络里是连不上的（TCP 直接被重置，不是超时），插件拿不到数据
/// 就会把整条链路报成失败 —— 例如 ZCode 国际版插件的模型清单取不到，
/// `catalog.refresh` 整个失败，账号一条模型都注册不上。
///
/// ── 换地址的边界 ──────────────────────────────────────────────
/// 只对**只读**（GET / HEAD）、**无请求体**、**不带凭证头**的请求换地址 ——
/// 账号令牌绝不经过第三方加速站。非 GitHub 域一律原样访问。
async fn plugin_targets(
    url: &url::Url,
    method: &reqwest::Method,
    headers: &[(String, String)],
    has_body: bool,
) -> Vec<url::Url> {
    let read_only = *method == reqwest::Method::GET || *method == reqwest::Method::HEAD;
    let credentialed = headers.iter().any(|(name, _)| {
        matches!(
            name.to_ascii_lowercase().as_str(),
            "authorization" | "cookie" | "proxy-authorization" | "x-api-key" | "api-key"
        )
    });
    if has_body || !read_only || credentialed {
        return vec![url.clone()];
    }
    // 非 GitHub 资源地址没有替身：换地址只针对 GitHub 域。
    if !crate::server::core::update::jsdelivr::is_github_resource(url.as_str()) {
        return vec![url.clone()];
    }
    // 线路选择在后台测速，当前请求不等它（测速最坏要 5 秒，挡住插件出网
    // 就是挡住首包）。没有结论时用默认线路 fastly —— 本机实测可用。
    crate::server::core::update::jsdelivr::ensure_in_background();
    let mut targets = Vec::new();
    // 官方地址优先，连不上再退到 jsDelivr 线路。
    targets.push(url.clone());
    if let Some(cdn) = crate::server::core::update::jsdelivr::raw_cdn(url.as_str()) {
        if let Ok(parsed) = url::Url::parse(&cdn) {
            targets.push(parsed);
        }
    }
    targets
}

pub fn resolve_proxy(params: &Value) -> Result<Value, proto::WireError> {
    let proxy = match resolve_account_proxy(params.get("config")) {
        Some(ProxyResolution::Resolved(proxy)) => Some(proxy),
        Some(ProxyResolution::Failed(message)) => return Err(error(message)),
        None => None,
    };
    Ok(match proxy {
        Some(proxy) => crate::server::core::proxies::chain::wire_json(&proxy).map_err(error)?,
        None => Value::Null,
    })
}

pub(super) fn is_loopback(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(host)) => host.trim_end_matches('.').eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback()
            || address.to_ipv4_mapped().is_some_and(|address| address.is_loopback()),
        None => false,
    }
}

pub(super) fn proxy_for(url: &url::Url, params: &Value) -> Result<Option<ResolvedProxy>, proto::WireError> {
    // 插件的本机浏览器控制与回调不能发往账号的远程代理。
    if is_loopback(url) { return Ok(None); }
    ResolvedProxy::from_json(params.get("proxy").unwrap_or(&Value::Null)).map_err(error)
}

pub async fn open(params: &Value) -> Result<Value, proto::WireError> {
    // 脚本 JSON 不是可信期限入口；不能悄悄忽略字段后声称已执行总期限。
    if params.get("timeout_ms").is_some() {
        return Err(proto::WireError::with_code("此入口不接受单请求总期限参数", "bad_params"));
    }
    let (result, handle) = open_with_client(params, None, None, false, HttpDeadline::default()).await?;
    handle.detach();
    Ok(result)
}

pub(in crate::server::core::plugin_host) enum RequestBody {
    Bytes(bytes::Bytes),
    Multipart(reqwest::multipart::Form),
}

pub(in crate::server::core::plugin_host) async fn open_body_with_policy(params: &Value, body: Option<RequestBody>,
    system_proxy_when_unset: bool, deadline: HttpDeadline) -> Result<(Value, HttpHandle), proto::WireError> {
    open_with_client(params, body, None, system_proxy_when_unset, deadline).await
}

// 客户端和期限都是宿主构造的类型化值，不能从插件 JSON 注入 DNS/代理/重定向/期限策略。
pub(in crate::server::core::plugin_host) async fn open_pinned(params: &Value, client: reqwest::Client,
    deadline: HttpDeadline) -> Result<(Value, HttpHandle), proto::WireError> {
    open_with_client(params, None, Some(client), false, deadline).await
}

pub(in crate::server::core::plugin_host) async fn open_pinned_body(params: &Value, body: Option<RequestBody>,
    client: reqwest::Client, deadline: HttpDeadline) -> Result<(Value, HttpHandle), proto::WireError> {
    open_with_client(params, body, Some(client), false, deadline).await
}

// 仅宿主可传整个冻结包装、原强执行占用和非Clone发送票；JSON不能替换它们。
// 本分支同步登记唯一worker后独立发送；外层只等结果，取消/trap不丢已execute的future。
pub(in crate::server::core::plugin_host) async fn open_frozen_once<F>(params: &Value, body: bytes::Bytes,
    owner: egress::managed::FrozenHttpClient, execution: Arc<WorkBuddyExecution>, deadline: HttpDeadline,
    headers_deadline: HttpDeadline, permit: F,
) -> Result<(Value, HttpHandle), proto::WireError>
where F: FnOnce(&reqwest::Request) -> Result<(), proto::WireError> + Send + 'static {
    frozen::open(params, body, owner, execution, deadline, headers_deadline, permit).await
}

async fn open_with_client(params: &Value, body: Option<RequestBody>, pinned: Option<reqwest::Client>,
    system_proxy_when_unset: bool, deadline: HttpDeadline) -> Result<(Value, HttpHandle), proto::WireError> {
    deadline.run(open_inner(params, body, pinned, system_proxy_when_unset, deadline)).await
}

async fn open_inner(params: &Value, body: Option<RequestBody>, pinned: Option<reqwest::Client>,
    system_proxy_when_unset: bool, deadline: HttpDeadline) -> Result<(Value, HttpHandle), proto::WireError> {
    deadline.check()?;
    let sensitive = pinned.is_some();
    let url = params["url"]
        .as_str()
        .ok_or_else(|| error("脚本请求缺少地址"))?;
    let url = url::Url::parse(url).map_err(|_| error("脚本请求地址无效"))?;
    if !matches!(url.scheme(), "https" | "http")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(error("脚本请求只允许不含账号信息的 HTTP 地址"));
    }
    let client = match pinned {
        Some(client) => client,
        None => {
            let proxy = proxy_for(&url, params)?;
            egress::resolved_client(proxy.as_ref(), system_proxy_when_unset && !is_loopback(&url), "", &[]).map_err(error)?
        }
    };
    deadline.check()?;
    let method = reqwest::Method::from_bytes(params["method"].as_str().unwrap_or("GET").as_bytes())
        .map_err(|_| error("脚本请求方法无效"))?;
    let multipart = matches!(body, Some(RequestBody::Multipart(_)));
    let mut sent_headers = Vec::new();
    if let Some(headers) = params["headers"].as_object() {
        if headers.len() > 128 {
            return Err(error("脚本请求头过多"));
        }
        for (key, value) in headers {
            if multipart && key.eq_ignore_ascii_case("content-type") { continue; }
            if matches!(
                key.to_ascii_lowercase().as_str(),
                "host"
                    | "connection"
                    | "content-length"
                    | "transfer-encoding"
                    | "proxy-authorization"
            ) {
                continue;
            }
            if let Some(value) = value.as_str() {
                sent_headers.push((key.clone(), value.to_string()));
            }
        }
    }
    let capture = params["trace"].as_str().and_then(super::trace::find);
    let mut sent_body = Value::Null;
    // 请求体先存着，等确定发往哪个候选地址时再挂上去 —— multipart 的表单流
    // 是一次性的，只能用一次，所以它不进重试也不进候选切换。
    let mut body_bytes: Option<bytes::Bytes> = None;
    let mut body_form: Option<reqwest::multipart::Form> = None;
    if let Some(body) = body {
        match body {
            // 资源承载的正文在调试开启时同样留痕；未开启不解析，避免额外复制。
            RequestBody::Bytes(bytes) => {
                if capture.is_some() {
                    sent_body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                }
                body_bytes = Some(bytes);
            }
            RequestBody::Multipart(form) => body_form = Some(form),
        }
    } else if let Some(body) = params["body"].as_str() {
        if body.len() > 120 * 1024 * 1024 {
            return Err(error("脚本请求体过大"));
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(body)
            .map_err(|_| error("脚本请求体编码无效"))?;
        deadline.check()?;
        if capture.is_some() {
            sent_body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        }
        // base64 解码得到 Vec<u8>，而 body_bytes 统一存 bytes::Bytes（可廉价 clone
        // 供多候选地址重试复用）——显式转换，避免合并遗留的类型不匹配。
        body_bytes = Some(bytes::Bytes::from(bytes));
    }
    deadline.check()?;
    let handle = lifecycle::register(params, deadline, capture.clone(), sensitive, system_proxy_when_unset)?;
    let download = &handle.download;
    let mut guard = download.wait(download.response.lock()).await?;
    let timeout = crate::server::config::timeout_settings()
        .headers_ms()
        .clamp(1000, 600_000);
    // 候选地址：GitHub 资源在官方地址连不上时改走 jsDelivr 线路（只读、无凭证
    // 头的请求才换 —— 账号令牌绝不经过第三方加速站）。
    let has_body = body_bytes.is_some() || body_form.is_some();
    let targets = plugin_targets(&url, &method, &sent_headers, has_body).await;
    let mut opened: Option<reqwest::Response> = None;
    let mut last_failure: Option<String> = None;
    // multipart 的请求体是一次性的（拼出来的流不能重放），那种情况
    // 不做退避重试；字节体和空体都可以重放。
    let retryable = body_form.is_none();
    'targets: for (index, target) in targets.iter().enumerate() {
        for attempt in 0..=RATE_LIMIT_RETRIES {
            let mut request = client.request(method.clone(), target.clone());
            for (key, value) in &sent_headers {
                request = request.header(key, value.as_str());
            }
            if let Some(bytes) = &body_bytes {
                request = request.body(bytes.clone());
            } else if let Some(form) = body_form.take() {
                request = request.multipart(form);
            }
            if let Some(capture) = &capture {
                capture.reset_request(
                    target.as_str(),
                    params["provider"].as_str().unwrap_or(""),
                    &sent_headers,
                    &sent_body,
                );
            }
            // 超时与连接失败分开归类：前者是「等响应头等太久」，后者是
            // reqwest 的连接/传输错误。归类要拿到原始的 reqwest::Error，
            // 所以这一层只把超时翻成 WireError，连接错误留给下一段处理。
            let outcome = download.wait(
                tokio::time::timeout(Duration::from_millis(timeout), request.send()),
            ).await?
                .map_err(|_| {
                    if let Some(trace) = &download.usage_trace {
                        trace.failed(crate::server::core::usage_query::network::FailureKind::RequestTimeout,
                            "连接或等待响应头超时");
                    }
                    proto::WireError::with_code("脚本上游连接等待超时", "timeout")
                })?;
            let response = match outcome {
                Ok(response) => response,
                Err(failure) => {
                    let detail = egress::describe_error_detail(&failure);
                    if let (Some(trace), Some(kind)) = (&download.usage_trace,
                        crate::server::core::usage_query::network::classify(&failure, false)) {
                        trace.failed(kind, &detail);
                    }
                    last_failure = Some(if sensitive {
                        "受控账号服务器连接失败".to_string()
                    } else {
                        detail
                    });
                    break;
                }
            };
            // 429 = 上游边缘节点限流，请求没被转发到业务逻辑，重试是安全的。
            // 按它给的 `Retry-After` 等一等再试。
            if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
                && retryable
                && attempt < RATE_LIMIT_RETRIES
            {
                let wait = retry_after(response.headers()) + backoff_jitter();
                drop(response);
                tokio::time::sleep(wait).await;
                continue;
            }
            opened = Some(response);
            break;
        }
        if opened.is_some() {
            break 'targets;
        }
        if index + 1 >= targets.len() {
            break 'targets;
        }
    }
    let response = match opened {
        Some(response) => response,
        None => {
            let detail = last_failure.unwrap_or_else(|| "脚本上游请求失败".to_string());
            return Err(error(detail));
        }
    };
    if let Some(trace) = &download.usage_trace { trace.response(response.status().as_u16()); }
    if let Some(capture) = &capture {
        capture.attach_response(response.status().as_u16(), response.headers());
    }
    let headers: serde_json::Map<String, Value> = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), Value::String(value.to_string())))
        })
        .collect();
    // 按原始条目保留同名头；Set-Cookie 不能合并或按逗号拆分。
    let headers_raw: Vec<(String, String)> = response.headers().iter().filter_map(|(name, value)| {
        value.to_str().ok().map(|value| (name.as_str().to_string(), value.to_string()))
    }).collect();
    let result = json!({ "id": handle.id(), "status": response.status().as_u16(), "headers": headers,
        "headers_raw": headers_raw, "url": response.url().as_str() });
    download.check()?;
    *guard = Some(response);
    download.check()?;
    drop(guard);
    Ok((result, handle))
}

pub async fn read(params: &Value) -> Result<Value, proto::WireError> {
    let id = params["id"].as_str().ok_or_else(|| error("缺少脚本网络会话"))?;
    let download = lifecycle::find(id)?;
    if download.holds_frozen() { return Err(error("封闭HTTP响应只允许原Context持有的具体对象读取")); }
    read_download(id, &download).await
}

async fn read_download(id: &str, download: &Arc<Download>) -> Result<Value, proto::WireError> {
    let result = read_inner(download).await;
    let done = result.as_ref().is_ok_and(|value| value["done"] == true);
    let result = if done { result } else { match download.check() { Ok(()) => result, Err(error) => Err(error) } };
    if result.is_err() || done {
        // 包括拿锁、pending、取消等早退错误，不能只删 Context 归属而留下全局下载。
        lifecycle::close_matching(id, download);
    }
    result
}

async fn read_inner(download: &Download) -> Result<Value, proto::WireError> {
    download.check()?;
    *download.wait(download.touched.lock()).await? = std::time::Instant::now();
    download.check()?;
    let mut guard = download.wait(download.response.lock()).await?;
    download.check()?;
    let cached = {
        let mut pending = download.wait(download.pending.lock()).await?;
        download.check()?;
        let encoded = pending.read_encoded(download.chunk_limit)?;
        download.check()?;
        encoded
    };
    download.check()?;
    if let Some(data) = cached {
        let result = json!({ "data": data, "done": false });
        download.check()?;
        return Ok(result);
    }
    let response = guard.as_mut().ok_or_else(|| error("脚本上游响应头尚未就绪"))?;
    let wait = crate::server::config::timeout_settings().stream_idle_ms().clamp(1000, 1_800_000);
    let result = download.wait(tokio::time::timeout(Duration::from_millis(wait), response.chunk())).await?;
    match result {
        Ok(Ok(Some(bytes))) => {
            if let Some(capture) = download.capture().await? { capture.push(&bytes); }
            download.check()?;
            if download.chunk_limit != usize::MAX && bytes.len() > pending::MAX_BYTES {
                return Err(error("上游单次网络分片过大"));
            }
            let size = bytes.len().min(download.chunk_limit);
            if size < bytes.len() {
                let mut pending = download.wait(download.pending.lock()).await?;
                download.check()?;
                pending.store(&bytes[size..])?;
                download.check()?;
            }
            download.check()?;
            let data = base64::engine::general_purpose::STANDARD.encode(&bytes[..size]);
            drop(bytes);
            download.check()?;
            let result = json!({ "data": data, "done": false });
            download.check()?;
            Ok(result)
        }
        Ok(Ok(None)) => {
            download.finish()?;
            if let Some(trace) = &download.usage_trace { trace.finish(); }
            Ok(json!({ "done": true }))
        }
        Ok(Err(failure)) => {
            if let (Some(trace), Some(kind)) = (&download.usage_trace,
                crate::server::core::usage_query::network::classify(&failure, true)) {
                trace.failed(kind, &egress::describe_error_detail(&failure));
            }
            Err(if download.sensitive { error("受控账号服务器响应中断") }
                else { error(egress::describe_error_detail(&failure)) })
        }
        Err(_) => {
            if let Some(trace) = &download.usage_trace {
                trace.failed(crate::server::core::usage_query::network::FailureKind::ResponseTimeout,
                    "读取上游响应超时");
            }
            Err(proto::WireError::with_code("脚本上游响应空闲超时", "timeout"))
        }
    }
}

pub fn close(params: &Value) {
    if let Some(id) = params["id"].as_str() { lifecycle::close_id(id); }
}

pub fn clear() {
    lifecycle::clear_scripts();
    super::protocol::clear();
    super::websocket::clear();
}

pub fn convert(params: &Value) -> Result<Value, proto::WireError> {
    let body = &params["body"];
    let model = params["model"].as_str().unwrap_or("");
    match params["protocol"].as_str().unwrap_or("chat") {
        "anthropic" => {
            crate::server::core::protocol::anthropic_outbound::anthropic_request_from_chat(
                body, model,
            )
            .map_err(error)
        }
        "responses" => {
            crate::server::core::protocol::responses_outbound::responses_request_from_chat(
                body, model,
            )
            .map_err(error)
        }
        "chat" => {
            let mut body = body.clone();
            body["model"] = json!(model);
            Ok(body)
        }
        _ => Err(proto::WireError::with_code(
            "脚本插件使用了尚未支持的模型协议",
            proto::WireError::UNSUPPORTED,
        )),
    }
}
