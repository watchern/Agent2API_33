use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use agent2api_provider_protocol as proto;
use base64::Engine;
use serde_json::{json, Value};

use crate::server::core::{egress, proxies::{ResolvedProxy, ProxyResolution, resolve_account_proxy}};

struct Download {
    response: tokio::sync::Mutex<Option<reqwest::Response>>,
    pending: Mutex<bytes::Bytes>,
    chunk_limit: usize,
    touched: Mutex<std::time::Instant>,
    cancel: tokio_util::sync::CancellationToken,
    capture: Option<Arc<crate::server::core::debug_traffic::TrafficCapture>>,
}

fn downloads() -> &'static Mutex<HashMap<String, Arc<Download>>> {
    static ITEMS: OnceLock<Mutex<HashMap<String, Arc<Download>>>> = OnceLock::new();
    ITEMS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn error(message: impl Into<String>) -> proto::WireError {
    proto::WireError::with_code(message, proto::WireError::INTERNAL)
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

struct Opening {
    id: Option<String>,
}

impl Drop for Opening {
    fn drop(&mut self) {
        if let Some(id) = &self.id {
            close(&json!({ "id": id }));
        }
    }
}

fn cancellation() -> proto::WireError {
    proto::WireError::with_code("脚本上游请求已取消", proto::WireError::ABORTED)
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

pub(super) fn proxy_for(url: &url::Url, params: &Value) -> Result<Option<ResolvedProxy>, proto::WireError> {
    let local = match url.host() {
        Some(url::Host::Domain(host)) => host.trim_end_matches('.').eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback()
            || address.to_ipv4_mapped().is_some_and(|address| address.is_loopback()),
        None => false,
    };
    // 插件的本机浏览器控制与回调不能发往账号的远程代理。
    if local { return Ok(None); }
    ResolvedProxy::from_json(params.get("proxy").unwrap_or(&Value::Null)).map_err(error)
}

/// 这条请求可以尝试的地址列表。
///
/// 只有「只读、无请求体、不带凭证头」的请求才允许换地址，判据三条：
///   · **只读方法**（GET / HEAD）—— 换地址不需要重放请求体；
///   · **无请求体** —— 带正文的请求换地址要把正文再发一遍，代价与风险都不
///     划算（multipart 的流更是只能拼一次）；
///   · **不带凭证头** —— 替身是第三方站点，账号令牌绝不能发过去。
///
/// 其余情况只有一个候选（原地址），行为与改动前完全一致。
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
    if !crate::server::core::update::source::is_resource(url.as_str()) {
        return vec![url.clone()];
    }
    // 线路选择在后台测速，当前请求不等它（测速最坏要 5 秒，挡住插件出网
    // 就是挡住首包）。没有结论时用默认线路 fastly —— 本机实测可用。
    crate::server::core::update::jsdelivr::ensure_in_background();
    let mut targets = Vec::new();
    for candidate in crate::server::core::update::source::plugin_candidates(url.as_str()) {
        if let Ok(parsed) = url::Url::parse(&candidate) {
            targets.push(parsed);
        }
    }
    if targets.is_empty() {
        vec![url.clone()]
    } else {
        targets
    }
}

pub async fn open(params: &Value) -> Result<Value, proto::WireError> {
    open_body(params, None).await
}

pub(in crate::server::core::plugin_host) enum RequestBody {
    Bytes(bytes::Bytes),
    Multipart(reqwest::multipart::Form),
}

pub(in crate::server::core::plugin_host) async fn open_body(params: &Value, body: Option<RequestBody>) -> Result<Value, proto::WireError> {
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
    let proxy = proxy_for(&url, params)?;
    let client = egress::resolved_client(proxy.as_ref(), false, "", &[]).map_err(error)?;
    let method = reqwest::Method::from_bytes(params["method"].as_str().unwrap_or("GET").as_bytes())
        .map_err(|_| error("脚本请求方法无效"))?;
    // 请求头先收成列表，不直接挂到 RequestBuilder 上：GitHub 替身回退要按
    // 候选地址重建请求，而 reqwest 的 RequestBuilder 既不可克隆、也不能在
    // 失败之后改地址。
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
    // 请求体拆成「可复制的字节」与「只能拼一次的 multipart」两半：只有前者
    // 能跟着替身地址重试（multipart 拼出来的流是一次性的）。
    let mut body_bytes: Option<bytes::Bytes> = None;
    let mut body_form: Option<reqwest::multipart::Form> = None;
    if let Some(body) = body {
        match body {
            RequestBody::Bytes(bytes) => body_bytes = Some(bytes),
            RequestBody::Multipart(form) => body_form = Some(form),
        }
    } else if let Some(body) = params["body"].as_str() {
        if body.len() > 120 * 1024 * 1024 {
            return Err(error("脚本请求体过大"));
        }
        let raw = base64::engine::general_purpose::STANDARD
            .decode(body)
            .map_err(|_| error("脚本请求体编码无效"))?;
        if capture.is_some() {
            sent_body = serde_json::from_slice(&raw).unwrap_or(Value::Null);
        }
        body_bytes = Some(bytes::Bytes::from(raw));
    }
    // 候选地址：GitHub 资源在官方地址连不上时改走 jsDelivr 线路 / 加速前缀
    // （口径见 `core::update::source::plugin_candidates`）。
    let targets = plugin_targets(&url, &method, &sent_headers, body_bytes.is_some() || body_form.is_some()).await;
    let id = params["id"]
        .as_str()
        .filter(|id| id.len() <= 80 && !id.is_empty())
        .ok_or_else(|| error("缺少脚本请求标识"))?
        .to_string();
    let download = Arc::new(Download {
        response: tokio::sync::Mutex::new(None),
        pending: Mutex::new(bytes::Bytes::new()),
        chunk_limit: params["max_chunk"].as_u64().map(|size| size.clamp(1024, 256 * 1024) as usize).unwrap_or(usize::MAX),
        touched: Mutex::new(std::time::Instant::now()),
        cancel: tokio_util::sync::CancellationToken::new(),
        capture: capture.clone(),
    });
    {
        let mut items = downloads()
            .lock()
            .map_err(|_| error("脚本网络会话不可用"))?;
        items.retain(|_, entry| {
            let keep = entry
                .touched
                .lock()
                .is_ok_and(|time| time.elapsed() < Duration::from_secs(1800));
            if !keep {
                entry.cancel.cancel();
            }
            keep
        });
        if items.len() >= 128 || items.contains_key(&id) {
            return Err(error("脚本上游连接数已达上限或请求标识重复"));
        }
        items.insert(id.clone(), download.clone());
    }
    let mut opening = Opening {
        id: Some(id.clone()),
    };
    let timeout = crate::server::config::timeout_settings()
        .headers_ms()
        .clamp(1000, 600_000);
    let response = {
        // 依次尝试候选地址：官方地址连不上时（部分网络里 GitHub 原始地址的
        // TCP 直接被重置，不是超时）换替身地址再试一次。
        let mut last: Option<reqwest::Error> = None;
        let mut opened: Option<reqwest::Response> = None;
        for (index, target) in targets.iter().enumerate() {
            // multipart 的请求体是一次性的（拼出来的流不能重放），那种情况
            // 不做退避重试；字节体和空体都可以重放。
            let retryable = body_form.is_none();
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
                let outcome = tokio::select! {
                    biased;
                    _ = download.cancel.cancelled() => return Err(cancellation()),
                    response = tokio::time::timeout(Duration::from_millis(timeout), request.send()) => match response {
                        Ok(Ok(response)) => Ok(response),
                        Ok(Err(failure)) => Err(failure),
                        Err(_) => return Err(proto::WireError::with_code("脚本上游连接等待超时", "timeout")),
                    },
                };
                match outcome {
                    Ok(response) => {
                        // 429 = 上游边缘节点限流，请求没被转发到业务逻辑，
                        // 重试是安全的。按它给的 `Retry-After` 等一等再试。
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
                    Err(failure) => {
                        last = Some(failure);
                        break;
                    }
                }
            }
            if opened.is_some() {
                break;
            }
            if index + 1 >= targets.len() {
                break;
            }
        }
        match opened {
            Some(response) => response,
            None => {
                let failure = last.ok_or_else(|| error("脚本上游请求失败"))?;
                return Err(error(egress::describe_error_detail(&failure)));
            }
        }
    };
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
    let result = json!({ "id": id, "status": response.status().as_u16(), "headers": headers, "url": response.url().as_str() });
    *download.response.lock().await = Some(response);
    opening.id = None;
    Ok(result)
}

pub async fn read(params: &Value) -> Result<Value, proto::WireError> {
    let id = params["id"]
        .as_str()
        .ok_or_else(|| error("缺少脚本网络会话"))?;
    let download = downloads()
        .lock()
        .map_err(|_| error("脚本网络会话不可用"))?
        .get(id)
        .cloned()
        .ok_or_else(|| error("脚本网络会话已结束"))?;
    if let Ok(mut time) = download.touched.lock() {
        *time = std::time::Instant::now();
    }
    let mut guard = download.response.lock().await;
    if download.cancel.is_cancelled() { return Err(cancellation()); }
    {
        let mut pending = download.pending.lock().map_err(|_| error("网络分片不可用"))?;
        if !pending.is_empty() {
            let size = pending.len().min(download.chunk_limit);
            let bytes = pending.split_to(size);
            return Ok(json!({ "data": base64::engine::general_purpose::STANDARD.encode(bytes), "done": false }));
        }
    }
    let response = guard
        .as_mut()
        .ok_or_else(|| error("脚本上游响应头尚未就绪"))?;
    let wait = crate::server::config::timeout_settings()
        .stream_idle_ms()
        .clamp(1000, 1_800_000);
    let result = tokio::select! {
        biased;
        _ = download.cancel.cancelled() => return Err(cancellation()),
        result = tokio::time::timeout(Duration::from_millis(wait), response.chunk()) => result,
    };
    match result {
        Ok(Ok(Some(mut bytes))) => {
            if let Some(capture) = &download.capture {
                capture.push(&bytes);
            }
            if download.chunk_limit != usize::MAX && bytes.len() > 4 * 1024 * 1024 {
                close(params);
                return Err(error("上游单次网络分片过大"));
            }
            if bytes.len() > download.chunk_limit {
                let rest = bytes.split_off(download.chunk_limit);
                *download.pending.lock().map_err(|_| error("网络分片不可用"))? = rest;
            }
            Ok(
                json!({ "data": base64::engine::general_purpose::STANDARD.encode(bytes), "done": false }),
            )
        }
        Ok(Ok(None)) => {
            close(params);
            Ok(json!({ "done": true }))
        }
        Ok(Err(failure)) => {
            close(params);
            Err(error(egress::describe_error_detail(&failure)))
        }
        Err(_) => {
            close(params);
            Err(proto::WireError::with_code("脚本上游响应空闲超时", "timeout"))
        }
    }
}

pub fn close(params: &Value) {
    if let Some(id) = params["id"].as_str() {
        if let Ok(mut items) = downloads().lock() {
            if let Some(item) = items.remove(id) {
                item.cancel.cancel();
            }
        }
    }
}

pub fn clear() {
    if let Ok(mut items) = downloads().lock() {
        items.retain(|id, item| {
            if id.starts_with("wasm-") { return true; }
            item.cancel.cancel();
            false
        });
    }
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
