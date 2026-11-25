//! 插件模型目录的同步快照；成功刷新持久化，装配时恢复后再使用握手兜底。

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use agent2api_provider_protocol as proto;
use serde_json::{json, Value};

use crate::server::core::providers::catalog_cache;
use crate::server::logging;

pub const SETTING_MODEL_CATALOG: &str = "modelCatalog";
// 非空信封保存已确认的空目录；标识不会作为模型广告出去。
const ENVELOPE_ID: &str = "__scoped_plugin_catalog_v1";
const ENVELOPE_VERSION: u64 = 1;

#[derive(Clone, Default)]
struct Entry {
    remote: Option<proto::ModelList>,
    fallback: Vec<Value>,
}

fn table() -> &'static Mutex<HashMap<String, Entry>> {
    static TABLE: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn write_gate() -> &'static Mutex<()> {
    static GATE: OnceLock<Mutex<()>> = OnceLock::new();
    GATE.get_or_init(|| Mutex::new(()))
}

fn scope(provider: &str) -> String {
    format!("plugin:{provider}")
}

fn accepts_empty(provider: &str) -> bool {
    crate::server::core::providers::is_scoped_runtime_provider(provider)
        || super::script::known_provider(provider)
        || super::wasm::owns(provider)
}

fn from_cached(provider: &str, cached: catalog_cache::CachedCatalog) -> Option<proto::ModelList> {
    if accepts_empty(provider) && cached.models.len() == 1 {
        let envelope = &cached.models[0];
        if envelope.get("id").and_then(Value::as_str) == Some(ENVELOPE_ID) {
            if envelope.get("version").and_then(Value::as_u64) != Some(ENVELOPE_VERSION)
                || envelope.get("provider").and_then(Value::as_str) != Some(provider)
            { return None; }
            let mut list: proto::ModelList = serde_json::from_value(envelope.get("list")?.clone()).ok()?;
            if !list.meta.remote || list.meta.refreshed_at < 0 { return None; }
            list.models = valid_models(list.models);
            return Some(list); // 已确认的空目录也恢复，不复活静态或上次模型。
        }
    }
    // ── 信封永远不是模型 ────────────────────────────────────────
    // `ENVELOPE_ID` 是 scoped 目录的**信封**，不是一条模型。它只在
    // `accepts_empty` 为真时才有意义（那边会把信封解包成真清单）；一旦这家
    // 不再是 scoped 执行体（换了插件形态 / 插件被换掉），缓存里那份信封就
    // 成了陈旧数据 —— 放行的话它会顶掉插件真正的模型清单，界面上只剩一个
    // 叫 `__scoped_plugin_catalog_v1` 的假模型，按真实模型名对话一律 404。
    let models = valid_models(cached.models);
    (!models.is_empty()).then_some(proto::ModelList {
        models,
        meta: proto::CatalogMeta { remote: true, refreshed_at: cached.fetched_at },
    })
}

fn restore(provider: &str) -> Option<proto::ModelList> {
    let key = scope(provider);
    if let Some(list) = catalog_cache::load(&key).and_then(|cached| from_cached(provider, cached)) {
        return Some(list);
    }
    let list = catalog_cache::load_legacy(provider).and_then(|cached| from_cached(provider, cached))?;
    // 写事务里再次检查新槽，避免与在线刷新竞争时覆盖它；旧槽保留供回退。
    catalog_cache::save_migrated(&key, catalog_cache::CachedCatalog {
        models: list.models,
        fetched_at: list.meta.refreshed_at,
    }).and_then(|cached| from_cached(provider, cached))
}

/// 去掉不能当模型广告出去的条目。
///
/// `ENVELOPE_ID`（scoped 目录的信封）**无条件**排除：它在 `accepts_empty`
/// 为真时由 [`from_cached`] 解包成真清单，为假时就是陈旧数据 —— 两种情形
/// 下它都不该作为一条模型出现在目录里。
fn valid_models(models: Vec<Value>) -> Vec<Value> {
    let mut claimed = HashSet::new();
    models.into_iter().filter(|model| {
        model.get("id").and_then(Value::as_str)
            .is_some_and(|id| id != ENVELOPE_ID && !id.trim().is_empty() && claimed.insert(id.to_ascii_lowercase()))
    }).collect()
}

pub fn set_defaults(provider: &str, models: Vec<Value>) -> Option<(usize, i64)> {
    let _write = accepts_empty(provider)
        .then(|| write_gate().lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
    // 只有「内存里还没有远端清单」时才需要从持久层恢复：已经有的话下面会
    // 直接丢弃恢复结果，别白读一次 DB（单连接锁，抢得很凶）。
    let needs_restore = table().lock().ok()
        .and_then(|map| map.get(provider).map(|entry| entry.remote.is_none()))
        .unwrap_or(true);
    let restored = needs_restore.then(|| restore(provider)).flatten();
    let fallback = valid_models(models);
    let mut map = table().lock().ok()?;
    let entry = map.entry(provider.to_string()).or_default();
    // ── 内容未变不推 generation（2026-11-25）────────────────────────
    // `set_defaults` 是**插件握手必经之路**：插件包里每家提供商装载时都会
    // 走一遍（bootstrap / script runtime）。原来这里无条件 `invalidate()`
    // 两次，于是「装一个 30 家的插件包」就把 generation 推高 60 格 ——
    // 而 manifest_cache / MODELS_CACHE / ValidationCache / routing_snapshot
    // 四张派生缓存全以 generation 为键，于是每轮请求都要重建全量模型
    // 目录（实测 /api/session 114-194 秒、/v1/models 首次 33 秒第二次
    // 116 秒）。旧版日志里 generation 从 1 涨到 274、装一次插件包 50 秒
    // 内涨 84 格，就是这么来的。
    // 现在只在目录内容真的变了时推：远端清单换了、或兜底清单换了。
    let before_remote = entry.remote.as_ref().map(|list| list.models.clone());
    let before_fallback = entry.fallback.clone();
    let recovered = if entry.remote.is_none() {
        let recovered = restored.as_ref().map(|list| (list.models.len(), list.meta.refreshed_at));
        entry.remote = restored;
        recovered
    } else {
        None
    };
    entry.fallback = fallback;
    let after_remote = entry.remote.as_ref().map(|list| list.models.clone());
    if before_remote != after_remote || before_fallback != entry.fallback {
        crate::server::core::providers::catalog::invalidate();
    }
    recovered
}

/// 本轮刷新拿到的清单与内存里的上一份是否逐字相同。
/// 只比 `models`：`refreshed_at` 只是展示用的刷新时刻，清单、绑定、聚合目录
/// 这些派生视图一个都不读它。
fn remote_models_unchanged(provider: &str, models: &[Value]) -> bool {
    let Ok(map) = table().lock() else { return false };
    map.get(provider)
        .and_then(|entry| entry.remote.as_ref())
        .is_some_and(|old| old.models.as_slice() == models)
}

pub fn set_remote(provider: &str, mut list: proto::ModelList) -> usize {
    let scoped = accepts_empty(provider);
    let _write = scoped.then(|| write_gate().lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
    if scoped && !list.meta.remote {
        return 0; // 静态或失败目录不能在持久层被重新标成远程成功。
    }
    list.models = valid_models(list.models);
    if !scoped && list.models.is_empty() {
        return 0;
    }
    list.meta.remote = true;
    if list.meta.refreshed_at <= 0 {
        list.meta.refreshed_at = logging::now_ms();
    }
    let count = list.models.len();
    let saved = if scoped {
        vec![json!({ "id": ENVELOPE_ID, "version": ENVELOPE_VERSION, "provider": provider, "list": list })]
    } else { list.models.clone() };
    // ── 内容未变不推 generation（2026-11-14 beta-2）────────────────
    // 后台目录刷新会周期性回到这里，绝大多数轮次拿到的清单与上一份逐字
    // 相同。原来无论内容变没变都无条件 `catalog::invalidate()` 两次，让
    // generation 在每轮 `/api/session` 期间漂移，连带打掉 manifest_cache /
    // MODELS_CACHE / provider_label 三张缓存（实测每轮重建 1 秒以上）。
    // 这里照常更新 refreshed_at、照常持久化，只是不推 generation：
    // 清单没变，所有以 generation 为键的派生结果本来就仍然有效。
    let unchanged = remote_models_unchanged(provider, &list.models);
    let bump = || {
        if !unchanged {
            crate::server::core::providers::catalog::invalidate();
        }
    };
    if !scoped {
        catalog_cache::save(&scope(provider), &saved, list.meta.refreshed_at);
        let Ok(mut map) = table().lock() else { return 0 };
        bump();
        map.entry(provider.to_string()).or_default().remote = Some(list);
        bump();
        return count;
    }
    let mut map = table().lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    bump();
    map.entry(provider.to_string()).or_default().remote = Some(list.clone());
    bump();
    // table 与 DB 不重叠持锁；scoped gate 仍保持跨请求的持久化写入顺序。
    drop(map);
    catalog_cache::save(&scope(provider), &saved, list.meta.refreshed_at);
    count
}

pub fn snapshot(provider: &str) -> Option<proto::ModelList> {
    if super::wasm::catalog::enabled(provider) { return super::wasm::catalog::aggregate(provider); }
    let map = table().lock().ok()?;
    let entry = map.get(provider)?;
    entry.remote.clone().or_else(|| {
        (!entry.fallback.is_empty()).then(|| proto::ModelList {
            models: entry.fallback.clone(),
            meta: proto::CatalogMeta::default(),
        })
    })
}

pub fn models_for(provider: &str) -> Option<Vec<Value>> {
    snapshot(provider).map(|list| list.models)
}

pub fn meta(provider: &str) -> proto::CatalogMeta {
    if super::wasm::catalog::enabled(provider) { return super::wasm::catalog::aggregate(provider).map(|list| list.meta).unwrap_or_default(); }
    let Ok(map) = table().lock() else { return proto::CatalogMeta::default() };
    map.get(provider).and_then(|entry| entry.remote.as_ref())
        .map(|list| list.meta.clone()).unwrap_or_default()
}

fn rename_list(models: &mut Vec<Value>, mapping: &HashMap<String, String>) -> usize {
    let mut changed = 0;
    for model in models.iter_mut() {
        let Some(id) = model.get("id").and_then(Value::as_str) else { continue };
        let Some(base) = mapping.get(&id.trim().to_ascii_lowercase()).cloned() else { continue };
        if !id.eq_ignore_ascii_case(&base) {
            model["id"] = Value::String(base);
            changed += 1;
        }
    }
    if changed > 0 {
        let mut seen = HashSet::new();
        models.retain(|model| {
            model
                .get("id")
                .and_then(Value::as_str)
                .map(|id| seen.insert(id.to_ascii_lowercase()))
                .unwrap_or(true)
        });
    }
    changed
}

/// 就地把插件缓存里的历史模型 ID 改成原始 ID（启动期后缀迁移用）。
/// 只改内存中的清单并持久化；真正的路由表由插件下一次刷新重建。
pub fn rename_models(provider: &str, mapping: &HashMap<String, String>) -> usize {
    if mapping.is_empty() {
        return 0;
    }
    let (changed, list) = {
        let Ok(mut map) = table().lock() else { return 0 };
        let Some(entry) = map.get_mut(provider) else { return 0 };
        let mut changed = 0;
        if let Some(remote) = entry.remote.as_mut() {
            changed += rename_list(&mut remote.models, mapping);
        }
        changed += rename_list(&mut entry.fallback, mapping);
        (changed, entry.remote.clone())
    };
    if changed > 0 {
        // 改名后内存里的清单已经与 `set_remote` 看到的「上一份」一致，
        // set_remote 自己不会再推 generation —— 这里显式推一次（只推一次）。
        crate::server::core::providers::catalog::invalidate();
        if let Some(list) = list {
            set_remote(provider, list);
        }
    }
    changed
}

pub fn forget(provider: &str) {
    let removed = table().lock().ok().and_then(|mut map| map.remove(provider));
    // 只有真的带走了内容才需要作废派生缓存：空条目被删掉不影响目录。
    if removed.is_some_and(|entry| entry.remote.is_some() || !entry.fallback.is_empty()) {
        crate::server::core::providers::catalog::invalidate();
    }
}

pub fn clear_all() {
    let had_content = table().lock().ok().map(|mut map| {
        let had = map.values().any(|entry| entry.remote.is_some() || !entry.fallback.is_empty());
        map.clear();
        had
    });
    if had_content == Some(true) {
        crate::server::core::providers::catalog::invalidate();
    }
}
