use std::sync::atomic::{AtomicU64, Ordering};

mod validation;

pub(crate) use validation::{ValidationCache, ValidationSnapshot};

use crate::server::config;
use crate::server::core::account_store::AccountStore;

static GENERATION: AtomicU64 = AtomicU64::new(0);

pub(crate) fn generation() -> u64 {
    GENERATION.load(Ordering::Acquire)
}

pub(crate) fn invalidate() {
    GENERATION.fetch_add(1, Ordering::AcqRel);
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
