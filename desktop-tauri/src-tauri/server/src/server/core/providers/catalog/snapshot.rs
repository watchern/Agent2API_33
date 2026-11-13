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
    // 展示名读的是「已安装包清单」这份落盘数据，这里的每个调用点
    // （配置变更 / 插件注册 / 模型目录替换）都意味着名单可能变了。
    // 顺手清掉 provider_label 的缓存，免得各调用点各自记着失效。
    crate::server::core::plugin_host::invalidate_provider_labels();
}

pub(crate) fn validation_snapshot(store: &AccountStore) -> std::sync::Arc<ValidationSnapshot> {
    validation::snapshot(store, store.validation_cache())
}

pub(crate) fn config_changed(before: &config::RuntimeConfig, after: &config::RuntimeConfig) -> bool {
    let keys = [crate::server::core::model_rules::KEY_MODEL_RULES,
        crate::server::core::custom_providers::KEY_CUSTOM_PROVIDERS, config::KEY_ROUTING_PROFILES];
    keys.iter().any(|key| before.raw().get(*key) != after.raw().get(*key))
}
