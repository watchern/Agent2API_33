//! 本机启动（desktop.launch.v1）：插件声明对本机 ZCode 客户端凭证的读写范围。
//!
//! fork 移植范围：清单声明层（declaration / validate_metadata / contract）完整对齐
//! beta.6，保证声明该能力的插件能过安装校验与握手。运行层（读凭证 / 写凭证 /
//! 启停客户端）依赖 beta.6 的 `local_session::Source`、`account_store::LaunchOrigin`
//! 与 `runtime_capacity` 安全底座，正在分批接入；未接入前这些方法返回明确错误。

pub(crate) mod declaration;
mod secret;

pub(crate) use secret::Secret;

use serde_json::{json, Value};

use super::manifest::Options;

pub(crate) fn validate_metadata(options: &Options, value: &Value) -> Result<(), String> {
    let metadata = &value[0]["desktop_launch"];
    match &options.manifest.desktop_launch {
        Some(declaration) if *metadata == declaration.summary() => Ok(()),
        None if metadata.is_null() => Ok(()),
        _ => Err("插件本机启动声明与权限清单不一致".into()),
    }
}

pub(crate) fn contract() -> Value {
    json!({ "capability": declaration::CAPABILITY, "entry": "local_desktop_confirmation",
        "methods": [declaration::CHECK, declaration::PREPARE], "storage": ["json_file", "sqlite", "keyring"],
        "platforms": ["windows"], "implicitExecution": false, "credentialsReturnedToUi": false })
}

pub(crate) fn wipe(value: &mut Value) {
    use zeroize::Zeroize;
    match value {
        Value::String(text) => text.zeroize(),
        Value::Array(values) => values.iter_mut().for_each(wipe),
        Value::Object(fields) => fields.values_mut().for_each(wipe),
        _ => { *value = Value::Null; },
    }
}
