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
