//! 聚合模型目录：提供商的原始清单、对外绑定及发送名解析。
//!
//! 原始模型 ID 是默认绑定，别名是额外绑定。各绑定独立开关；关闭默认绑定
//! 不会禁用它指向的上游模型，也不会影响仍开启的别名。
//! 清单来源在本文件，绑定解析在 routing.rs，对外与管理视图在 view.rs。

mod routing;
mod snapshot;
mod view;

pub(crate) use snapshot::{config_changed, generation, invalidate, validation_snapshot, ValidationCache};

use serde_json::Value;

use crate::server::core::account_store::AccountStore;
use crate::server::core::model_meta;
use crate::server::core::model_rules;
use crate::server::core::models::model_id;
use crate::server::core::providers::adapter::adapter_by_id;
use crate::server::core::providers::{kind_from_id, kind_id, ProviderKind, PROVIDERS};

pub(crate) use routing::entry_id_in_manifest;
pub use routing::{
    default_model_catalog, default_model_usable, forwarding_providers, model_blocked_everywhere,
    providers_for_model, wire_target_for_provider, WireTarget,
};
pub use view::{
    advertised_manifest_contains, advertised_model_ids, has_available_providers, manage_view,
    models_by_provider, models_response, session_models, suggest_advertised,
};
/// 「这条映射在管理页上还有没有模型行可挂」——删除自映射时的放行判据（见函数说明）。
/// 只服务写侧一个调用点，不进对外的 `pub use`。
pub(crate) use view::host_row_exists;

pub(crate) fn upstream_manifests() -> Vec<(String, Vec<Value>)> {
    all_provider_ids().into_iter().map(|provider| {
        let models = adapter_by_id(&provider).map(|adapter| adapter.list_models()).unwrap_or_default();
        (provider, models)
    }).collect()
}

/// 原始能力清单不应用绑定开关；关闭原始 ID 后，别名仍需解析到这条上游记录。
/// 用户自建模型、models.dev 补齐与能力覆盖都在这一处应用，插件与内置身份共用。
///
/// ── 按家缓存（2026-11-12 剖析结论）─────────────────────────
/// 本函数是全仓**最热的清单构建点**：10 个调用点里有选路预演、转发名解析
/// （`wire_target_for_provider`，每条转发请求都走）、聚合目录、路由组投影，
/// 而单次构建 = `adapter.list_models()`（插件走 rpc_cache）+ models.dev
/// 元数据逐模型应用 + 能力覆写——本机实测每家 ~3 秒、一轮聚合目录两次
/// 全量构建合计 ~50 秒（见 view.rs 的分段插桩）。按 (provider, generation)
/// 缓存：目录刷新（rpc_cache::set_remote/set_defaults）、models.dev 快照
/// 替换（model_meta::replace）、规则/自定义家变更（config_changed）都会
/// `catalog::invalidate()` 推 generation —— 推了缓存自然失效，语义与直构
/// 逐字相同。账号台账写不推 generation，不再打掉缓存。
///
/// 返回 clone：调用方全部只读（逐点核对过），clone 一次几微秒。
pub(crate) fn manifest_for_provider(provider: &str) -> Vec<Value> {
    let metadata = generation();
    {
        let Ok(cache) = manifest_cache().lock() else {
            return build_manifest_for_provider(provider);
        };
        if let Some((cached_generation, manifest)) = cache.get(provider) {
            if *cached_generation == metadata {
                return manifest.as_ref().clone();
            }
        }
    }
    let built = build_manifest_for_provider(provider);
    if let Ok(mut cache) = manifest_cache().lock() {
        // 家数有限（内置 + 已装插件），不设淘汰；generation 变化靠键比对失效。
        cache.insert(provider.to_string(), (metadata, std::sync::Arc::new(built.clone())));
    }
    built
}

fn build_manifest_for_provider(provider: &str) -> Vec<Value> {
    // 剖析插桩（2026-11-12）：单家构建实测 ~3 秒，但三段读下来都应是微秒级。
    // 把「list_models（插件 rpc_cache / wasm aggregate）」「custom+meta」「能力
    // 覆写」分开计时，超过 500ms 打 console——3 秒到底花在哪一段，一轮部署钉死。
    let started = std::time::Instant::now();
    let Some(adapter) = adapter_by_id(provider) else {
        return Vec::new();
    };
    let mut models = adapter.list_models();
    let list_at = started.elapsed();
    for item in model_rules::custom_models_for(provider) {
        let id = model_id(&item);
        if !id.is_empty()
            && !models.iter().any(|existing| model_id(existing).eq_ignore_ascii_case(&id))
        {
            let mut item = item;
            item["_manualModel"] = Value::Bool(true);
            models.push(item);
        }
    }
    for item in models.iter_mut() {
        model_meta::apply(item);
    }
    let meta_at = started.elapsed();
    model_rules::apply_capability_overrides(provider, &mut models);
    if started.elapsed().as_millis() > 500 {
        crate::server::logging::console(
            "[Models]",
            &format!(
                "清单构建 {}（list_models {} ms / 元数据+自定义 {} ms / 能力覆写 {} ms，{} 个模型）",
                provider,
                list_at.as_millis(),
                meta_at.as_millis(),
                started.elapsed().as_millis(),
                models.len(),
            ),
        );
    }
    models
}

/// 每家清单缓存：`(provider, (generation, manifest))`。
fn manifest_cache() -> &'static std::sync::Mutex<std::collections::HashMap<String, (u64, std::sync::Arc<Vec<Value>>)>> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, (u64, std::sync::Arc<Vec<Value>>)>>>
        = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// 编译期身份的调用方仍按 kind 查询，执行体优先取已安装的插件。
pub(crate) fn manifest_for(kind: ProviderKind) -> Vec<Value> {
    manifest_for_provider(kind_id(kind))
}

/// 旧内置目录已迁入插件，网关不再持有按 kind 划分的刷新元数据。
pub(crate) fn refresh_meta(_kind: ProviderKind) -> (bool, i64) {
    (false, 0)
}

pub(crate) fn refresh_meta_for_provider(provider: &str) -> (bool, i64) {
    if crate::server::core::plugin_host::serves_provider(provider) {
        let meta = crate::server::core::plugin_host::model_catalog_meta(provider);
        return (meta.remote, meta.refreshed_at);
    }
    kind_from_id(provider).map(refresh_meta).unwrap_or_default()
}

fn all_provider_ids() -> Vec<String> {
    let mut ids = super::executable_provider_ids();
    for provider in PROVIDERS {
        if !ids.iter().any(|id| id.eq_ignore_ascii_case(provider.id)) {
            ids.push(provider.id.to_string());
        }
    }
    ids
}

pub fn provider_available(store: &AccountStore, provider: &str) -> bool {
    store.routing_snapshot().available_providers.contains(provider)
}

/// 管理视图需要完整清单；对外视图在此基础上再应用每条绑定的开关。
pub(crate) fn managed_manifests(store: &AccountStore) -> Vec<(String, Vec<Value>)> {
    let accounts = store.routing_snapshot();
    all_provider_ids()
        .into_iter()
        .filter(|provider| accounts.available_providers.contains(provider))
        .map(|provider| {
            let manifest = manifest_for_provider(&provider);
            (provider, manifest)
        })
        .collect()
}

pub fn active_manifests(store: &AccountStore) -> Vec<(String, Vec<Value>)> {
    managed_manifests(store).into_iter().filter_map(|(provider, manifest)| {
        let manifest = adapter_by_id(&provider)
            .map(|adapter| adapter.advertise_models(store, manifest))
            .unwrap_or_default();
        (!manifest.is_empty()).then_some((provider, manifest))
    }).collect()
}

fn aggregate_source(active: &[(String, Vec<Value>)]) -> (&'static str, i64) {
    match active {
        [(provider, _)] => {
            let (remote, refreshed_at) = refresh_meta_for_provider(provider);
            (if remote { "remote" } else { "builtin" }, refreshed_at)
        }
        [] => ("none", 0),
        _ => ("aggregate", active.first().map(|(provider, _)| refresh_meta_for_provider(provider).1).unwrap_or(0)),
    }
}
