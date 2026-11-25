//! jsDelivr 三线路路由：替代 `raw.githubusercontent.com` 读取仓库文件。
//!
//! ── 为什么换 ──────────────────────────────────────────────────
//! 宿主与插件商店的索引、`app.json`、插件清单、图标、压缩包此前都打
//! `raw.githubusercontent.com`；该域在部分网络里被重置/阻断（本机实测
//! `curl` 直接 connection reset，更新页每轮刷「连接 GitHub 源超时」）。
//! jsDelivr 的 `/gh/{owner}/{repo}@{ref}/{path}` 提供同样的仓库文件内容，
//! 且有三条可互换的官方线路：
//!
//! | 线路 | 说明 |
//! | --- | --- |
//! | `cdn.jsdelivr.net`   | 主域（多 CDN 调度） |
//! | `fastly.jsdelivr.net` | Fastly 线路 |
//! | `gcore.jsdelivr.net`  | Gcore 线路 |
//!
//! 三条线路内容一致、域名不同，可单独测速。本机实测 `cdn` 不通、
//! `fastly` 3.58s、`gcore` 2.20s —— 因此「先测速再择优」比写死主域可用。
//!
//! ── 工作方式 ──────────────────────────────────────────────────
//! 1. 源码里的 URL 一律用**规范形态**（`cdn.jsdelivr.net` 主域）书写，
//!    便于阅读与核对路径；
//! 2. 真正发请求前 [`route`] 把规范域换成当前选中的线路（未测速时用
//!    [`DEFAULT_HOST`]，不让被墙的主域成为默认值）；
//! 3. [`ensure`] 并发探测三条线路，取**首个完整返回且耗时最短**的一条，
//!    结果缓存 [`TTL_MS`]；进入插件页 / 更新设置页时前端会强制复测，
//!    启动期或任何请求在缓存缺失/过期时也会惰性补测一次；
//! 4. 探测失败不覆盖上一次的成功选择 —— 网络抖动不该把已选中的好线路丢掉。
//!
//! ── 注意：jsDelivr 会缓存分支内容 ─────────────────────────────
//! `@main` / `@dist` 这类分支引用在 CDN 侧有缓存（数小时级），所以索引
//! 更新可能比 raw 源晚一段时间。这是接受 CDN 加速的固有取舍；需要即时
//! 读最新提交时应改用具名 tag / commit。本次只替换读取端点，不改业务语义。

use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

use futures::{future::join_all, StreamExt};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

/// 三条官方线路。顺序即探测时的展示顺序；索引 0 是规范主域。
pub const HOSTS: [&str; 3] = [
    "cdn.jsdelivr.net",
    "fastly.jsdelivr.net",
    "gcore.jsdelivr.net",
];

const CANONICAL_HOST: &str = HOSTS[0];
/// 尚未测速时的默认线路：不用主域 —— 部分网络里它恰好是被墙的那条。
const DEFAULT_HOST: &str = HOSTS[1];
/// 探测用的小文件（<2KB，三条线路都有）。
const PROBE_PATH: &str = "/gh/Agent2plugin/Agent2plugin@main/index.json";
const PROBE_TIMEOUT_MS: u64 = 5_000;
const PROBE_BYTES: usize = 2 * 1024;
/// 选择结果的有效期；过期后下一次请求惰性复测。
const TTL_MS: i64 = 30 * 60 * 1000;
/// 前端「进入页面强制复测」的最小间隔，避免来回切页把 CDN 打一遍。
const FORCE_MIN_INTERVAL_MS: i64 = 60_000;

struct Line {
    host: &'static str,
    ok: bool,
    elapsed_ms: u64,
    error: Option<String>,
}

struct Selection {
    host: &'static str,
    at: i64,
    elapsed_ms: u64,
    lines: Vec<Line>,
}

static SELECTION: OnceLock<RwLock<Option<Selection>>> = OnceLock::new();
static GATE: OnceLock<AsyncMutex<()>> = OnceLock::new();

fn selection() -> &'static RwLock<Option<Selection>> {
    SELECTION.get_or_init(|| RwLock::new(None))
}

fn gate() -> &'static AsyncMutex<()> {
    GATE.get_or_init(|| AsyncMutex::new(()))
}

fn now_ms() -> i64 {
    crate::server::logging::now_ms()
}

fn is_host(host: &str) -> bool {
    HOSTS.contains(&host) || host.ends_with(".jsdelivr.net")
}

/// 这是不是一条我们接管的 jsDelivr `/gh/` 仓库文件 URL。
pub fn is(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some_and(is_host)
            && url.path().starts_with("/gh/")
            && url.username().is_empty()
            && url.password().is_none()
    })
}

/// 这是不是一条 GitHub 资源地址（插件/宿主可能需要换线路的那一类）。
///
/// 口径与旧的 `core::update::source::is_resource` 一致，但不再依赖「GitHub
/// 访问方式」设置 —— 那套设置随镜像子系统一起移除了，现在只有一条固定规则：
/// **GitHub 域的资源地址允许换 jsDelivr 线路，其他站点一律原样访问**。
pub fn is_github_resource(value: &str) -> bool {
    let Ok(url) = url::Url::parse(value) else {
        return false;
    };
    if url.scheme() != "https"
        || url.port_or_known_default() != Some(443)
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return false;
    }
    match url.host_str().unwrap_or("") {
        "api.github.com" => url.path().starts_with("/repos/"),
        "raw.githubusercontent.com" | "codeload.github.com" => true,
        "github.com" | "www.github.com" => url.path().contains("/releases/download/"),
        // jsDelivr 仓库文件：三条线路同形，路径固定 /gh/{owner}/{repo}@{ref}/…
        _ if is(value) => true,
        host => host.ends_with(".githubusercontent.com"),
    }
}

/// 构造规范形态的 jsDelivr URL（源码里的常量都走这里）。
///
/// `reference` 为空或 `HEAD` 时落到 `main`：jsDelivr 不认 `HEAD` 这个 ref，
/// 而 raw 源支持；调用方拿不到默认分支时给一个可用的兜底。
pub fn gh(repository: &str, reference: &str, path: &str) -> String {
    let reference = match reference {
        "" | "HEAD" => "main",
        other => other,
    };
    format!(
        "https://{CANONICAL_HOST}/gh/{repository}@{reference}/{}",
        path.trim_start_matches('/')
    )
}

/// `raw.githubusercontent.com` 的仓库文件地址 → jsDelivr `/gh/` 线路地址。
///
/// 形态：
/// `https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{path}`
/// → `https://{线路}/gh/{owner}/{repo}@{ref}/{path}`
///
/// ── 为什么插件侧也需要这条 ────────────────────────────────────
/// 宿主自己的仓库文件读取早就走 jsDelivr 了（见本模块头），但**插件在运行时
/// 自己拼出来的地址**（模型目录、官方配置、索引文件……）仍然直打
/// `raw.githubusercontent.com`。该域在部分网络里是连不上的（TCP 直接被重置，
/// 不是超时），插件拿不到数据就会把整条链路报成失败 —— 例如 ZCode 国际版
/// 插件的模型清单取不到，`catalog.refresh` 整个失败，账号一条模型都注册不上。
///
/// ── 为什么不改写而是回退 ──────────────────────────────────────
/// 改写等于「永远走 CDN」：CDN 挂了而官方源恰好能通时，反而把本来好用的
/// 路径弄坏。所以这里只给出**替身地址**，由调用方在官方地址连接失败后再试
/// （见 `core::update::source::plugin_candidates`）。
///
/// 不是上面这个形态的地址（别的域、ref 里带 `/`、空路径、`..` 段）一律返回
/// `None` —— 宁可不改写，也不要拼出一条打不开的 URL。
pub fn raw_cdn(value: &str) -> Option<String> {
    let url = url::Url::parse(value).ok()?;
    if url.host_str() != Some("raw.githubusercontent.com")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    let segments: Vec<&str> = url.path().trim_start_matches('/').split('/').collect();
    if segments.len() < 4 { return None; }
    let (owner, repository, reference) = (segments[0], segments[1], segments[2]);
    let path = segments[3..].join("/");
    if owner.is_empty() || repository.is_empty() || reference.is_empty() || path.is_empty() {
        return None;
    }
    // ref 里带 `/`（`refs/heads/main` 这类）拼不成 jsDelivr 的 `@ref` 形态。
    if reference.contains('/') {
        return None;
    }
    if path.split('/').any(|part| part.is_empty() || part == "." || part == "..") {
        return None;
    }
    Some(route(&gh(&format!("{owner}/{repository}"), reference, &path)))
}

/// 后台惰性测速：当前请求**不等**它。
///
/// ── 为什么不在出网路径上等 ────────────────────────────────────
/// 测速要等三条线路各回一个探测文件，最坏 5 秒。插件出网是热路径（每次
/// 插件发 HTTP 都要过），让它挡在请求前面会直接吃掉首包延迟。
///
/// 没有测速结果时 [`route`] 用 [`DEFAULT_HOST`] —— 那条线路本机实测可用，
/// 所以「先发、用默认线路，后台再测速择优」是安全的。
pub fn ensure_in_background() {
    let cached = selection()
        .read()
        .ok()
        .and_then(|guard| guard.as_ref().map(|value| value.at));
    if cached.is_some_and(|at| now_ms().saturating_sub(at) < TTL_MS) {
        return;
    }
    tokio::spawn(async move {
        ensure(false).await;
    });
}

fn selected_host() -> Option<&'static str> {
    let guard = selection().read().ok()?;
    let value = guard.as_ref()?;
    (now_ms().saturating_sub(value.at) < TTL_MS).then_some(value.host)
}

/// 发请求前调用：把规范主域换成当前选中的线路。
///
/// 非规范域（探测自己的 `fastly` / `gcore` URL）原样放行，否则测速时会被
/// 又换回主域，永远测不到另外两条。
pub fn route(value: &str) -> String {
    let Ok(mut url) = url::Url::parse(value) else {
        return value.to_string();
    };
    if url.host_str() != Some(CANONICAL_HOST) || !url.path().starts_with("/gh/") {
        return value.to_string();
    }
    let host = selected_host().unwrap_or(DEFAULT_HOST);
    if host != CANONICAL_HOST {
        let _ = url.set_host(Some(host));
    }
    url.to_string()
}

/// 当前测速结论（供设置页显示；未测过时给 `selected: null`）。
pub fn status() -> Value {
    let Ok(guard) = selection().read() else {
        return json!({ "selected": Value::Null, "checkedAt": 0, "lines": [] });
    };
    let Some(value) = guard.as_ref() else {
        return json!({ "selected": Value::Null, "checkedAt": 0, "lines": [] });
    };
    json!({
        "selected": value.host,
        "elapsedMs": value.elapsed_ms,
        "checkedAt": value.at,
        "ttlMs": TTL_MS,
        "expired": now_ms().saturating_sub(value.at) >= TTL_MS,
        "lines": value.lines.iter().map(|line| json!({
            "host": line.host,
            "ok": line.ok,
            "elapsedMs": line.elapsed_ms,
            "error": line.error,
        })).collect::<Vec<_>>(),
    })
}

/// 保证有一条可用线路；`force=true` 是前端进入页面时的强制复测。
///
/// 并发调用由 [`gate`] 合并：账号页/插件页/定时检查可能同时发请求，只让
/// 第一个真正打三条线路，其余等它写完缓存后直接复用。
pub async fn ensure(force: bool) -> Value {
    let _guard = gate().lock().await;
    let cached = selection().read().ok().and_then(|guard| guard.as_ref().map(|value| (value.host, value.at)));
    let fresh = cached.is_some_and(|(_, at)| now_ms().saturating_sub(at) < TTL_MS);
    let too_soon = force && cached.is_some_and(|(_, at)| now_ms().saturating_sub(at) < FORCE_MIN_INTERVAL_MS);
    if (fresh && !force) || too_soon {
        return status();
    }
    let lines = probe_all().await;
    let available = lines.iter().filter(|line| line.ok).count();
    let winner = lines.iter().filter(|line| line.ok)
        .min_by_key(|line| line.elapsed_ms)
        .map(|line| (line.host, line.elapsed_ms));
    if let Some((host, elapsed_ms)) = winner {
        if let Ok(mut guard) = selection().write() {
            *guard = Some(Selection { host, at: now_ms(), elapsed_ms, lines });
        }
        crate::server::logging::log(
            "[jsDelivr]",
            &format!(
                "测速完成：选用 {}（{} ms，{}/{} 条线路可用）",
                host, elapsed_ms, available, HOSTS.len(),
            ),
        );
    } else {
        // 三条全失败：保留上一次的选择（也许只是这一轮网络抖动），日志留痕。
        crate::server::logging::log_with_level(
            "[jsDelivr]",
            &format!(
                "三条线路本次均不可用（{}），沿用上次选择",
                lines.iter().filter_map(|line| line.error.as_deref()).collect::<Vec<_>>().join(" / ")
            ),
            "warn",
        );
    }
    status()
}

async fn probe_all() -> Vec<Line> {
    // 与正式请求用同一个出网代理，测速结论才对真实下载有效。
    let proxy = super::client::resolve_egress_candidates()
        .ok()
        .and_then(|candidates| candidates.into_iter().next())
        .and_then(|(_, proxy)| proxy);
    let client = crate::server::core::egress::client_for(proxy.as_ref());
    join_all(HOSTS.iter().map(|host| {
        let client = client.clone();
        async move {
            let url = format!("https://{host}{PROBE_PATH}");
            let started = Instant::now();
            let outcome = tokio::time::timeout(Duration::from_millis(PROBE_TIMEOUT_MS), async {
                let response = client
                    .get(&url)
                    .timeout(Duration::from_millis(PROBE_TIMEOUT_MS))
                    .send()
                    .await
                    .map_err(|error| error.to_string())?;
                if !response.status().is_success() {
                    return Err(format!("HTTP {}", response.status().as_u16()));
                }
                let mut bytes = Vec::new();
                let mut stream = response.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|error| error.to_string())?;
                    bytes.extend_from_slice(&chunk);
                    if bytes.len() >= PROBE_BYTES {
                        break;
                    }
                }
                if bytes.is_empty() {
                    return Err("空响应".to_string());
                }
                if String::from_utf8_lossy(&bytes).trim_start().starts_with('<') {
                    return Err("返回了网页而不是文件内容".to_string());
                }
                Ok::<(), String>(())
            })
            .await;
            let elapsed_ms = started.elapsed().as_millis() as u64;
            match outcome {
                Ok(Ok(())) => Line { host, ok: true, elapsed_ms, error: None },
                Ok(Err(error)) => Line { host, ok: false, elapsed_ms, error: Some(error) },
                Err(_) => Line { host, ok: false, elapsed_ms, error: Some("探测超时".to_string()) },
            }
        }
    }))
    .await
}
