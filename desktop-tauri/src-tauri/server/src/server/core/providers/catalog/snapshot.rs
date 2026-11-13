use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

mod validation;

pub(crate) use validation::{ValidationCache, ValidationSnapshot};

use crate::server::config;
use crate::server::core::account_store::AccountStore;

static GENERATION: AtomicU64 = AtomicU64::new(0);
/// 调用点采样的限频戳（5 秒最多一行 console，绝不入库）。
static LAST_CALLER_LOG: AtomicI64 = AtomicI64::new(0);

pub(crate) fn generation() -> u64 {
    GENERATION.load(Ordering::Acquire)
}

/// 推高目录 generation。
///
/// 2026-11-14 beta-4：稳态里 `MODELS_CACHE` / `manifest_cache` /
/// `routing_snapshot` 每轮都命中不了，怀疑 generation 仍在后台被反复推高。
/// `#[track_caller]` 让采样日志直接给出**调用点的文件与行号**，不必逐处
/// 猜；console-only 且 5 秒限频，正常配置改动最多留一行，不刷屏。
#[track_caller]
pub(crate) fn invalidate() {
    let next = GENERATION.fetch_add(1, Ordering::AcqRel) + 1;
    let caller = std::panic::Location::caller();
    let now = crate::server::logging::now_ms();
    let last = LAST_CALLER_LOG.load(Ordering::Relaxed);
    if now.saturating_sub(last) >= 5_000
        && LAST_CALLER_LOG
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        crate::server::logging::console(
            "[Catalog]",
            &format!(
                "generation → {}（调用点 {}:{}，5 秒采样一次）",
                next,
                caller.file(),
                caller.line(),
            ),
        );
    }
    // ── 这里**不要**顺手清 provider_label 缓存（2026-11-14 beta-2）──
    // 展示名只依赖「已安装包清单」，跟模型目录 generation 无关；
    // 而本函数被后台目录刷新高频调用，与 `/api/session` 并发时会把缓存
    // 打掉，列表里逐条 `label_of` 又各自去扫一遍 `list_packs()`（磁盘
    // I/O）—— 首屏耗时随账号数线性增长的正是这一段。
    // 真正的失效出口只有两个：插件包装卸 / 启停（`PackRegistry::save`）
    // 与脚本注册表写侧（`script::storage::save`），二者已显式调用
    // `plugin_host::invalidate_provider_labels()`。
}

pub(crate) fn validation_snapshot(store: &AccountStore) -> std::sync::Arc<ValidationSnapshot> {
    validation::snapshot(store, store.validation_cache())
}

pub(crate) fn config_changed(before: &config::RuntimeConfig, after: &config::RuntimeConfig) -> bool {
    let keys = [crate::server::core::model_rules::KEY_MODEL_RULES,
        crate::server::core::custom_providers::KEY_CUSTOM_PROVIDERS, config::KEY_ROUTING_PROFILES];
    keys.iter().any(|key| before.raw().get(*key) != after.raw().get(*key))
}
