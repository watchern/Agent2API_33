//! 套餐池验证码库存端点。插件维护模型请求免验证状态与近期需求，
//! 面板只读取状态；令牌由用户主动完成验证后提交，不自动打开验证码。
//! 领取验证与模型请求验证使用独立的上游配置字段。

use axum::body::Bytes;
use axum::extract::State;
use axum::response::Response;
use serde_json::{json, Value};

use crate::server::config;
use crate::server::core::plugin_host::{self, wasm::verification};
use crate::server::core::providers::{adapter, zcode};
use crate::server::errors::management_error;
use crate::server::http::{ok_json, parse_body};
use crate::server::logging;
use crate::server::ServerState;

/// 全局令牌入池不需要账号；读取配置时按当前启用的套餐池账号选地区。
async fn call_action(state: &ServerState, account_id: &str, action: &str, payload: &Value) -> Result<Value, (i32, String)> {
    let adapter = if account_id.is_empty() {
        zcode::region::Region::ALL.into_iter()
            .find_map(|region| crate::server::core::providers::adapter::adapter_by_id(region.provider_id()))
    } else {
        state.store().account_provider_of(account_id)
            .and_then(|provider| crate::server::core::providers::adapter::adapter_by_id(&provider))
    };
    let Some(adapter) = adapter else {
        return Err((
            400,
            "ZCode 的提供商插件没有安装或未启用，无法访问验证码令牌池".to_string(),
        ));
    };
    adapter
        .admin_action(state.store(), account_id, action, payload)
        .await
        .map_err(|error| (error.status_code, error.message))
}

/// 令牌提交时核对声称的 provider 与账号真实归属；不使用公开视图构建路径，
/// 避免为了核对归属去解析代理或读取无关字段。
pub(super) fn account_provider(state: &ServerState, account_id: &str, requested: Option<&str>) -> Result<String, (i32, String)> {
    if account_id.is_empty() || account_id.len() > 512 || account_id.chars().any(char::is_control) {
        return Err((400, "请明确验证码所属账号".into()));
    }
    let record = state.store().route_account(account_id).ok_or_else(|| (404, "找不到该 ZCode 账号".into()))?;
    let provider = record["provider"].as_str().unwrap_or("");
    if zcode::region::Region::from_provider_id(provider).is_none() {
        return Err((400, "该账号不是 ZCode 账号".into()));
    }
    if requested.is_some_and(|requested| requested != provider) {
        return Err((400, "验证码提供商与账号真实归属不一致".into()));
    }
    if !plugin_host::serves_provider(provider)
        || plugin_host::host_for_provider(provider).is_none_or(|host| !host.ready()) {
        return Err((503, "验证码执行体尚未就绪".into()));
    }
    Ok(provider.to_string())
}

/// 库存概况补上网关的显示参数、账号计数和近期请求需求。
async fn stats_json(state: &ServerState) -> Value {
    let (start_plan_accounts, account_id) = start_plan_accounts(state);
    let mut body = match call_action(state, account_id.as_deref().unwrap_or(""), "captcha.stats", &Value::Null).await {
        Ok(body) => body,
        // 插件不在时给一张**如实**的空表（而不是 500）：界面据此显示
        // 「不可用」，铸造器也就不会白烧阿里云配额。
        Err((_status, message)) => json!({ "ready": 0, "required": false, "error": message }),
    };
    let ready = body.get("ready").and_then(Value::as_u64).unwrap_or(0);
    let required = body.get("required").and_then(Value::as_bool).unwrap_or(false);
    let demand_active = body.get("demandActive").and_then(Value::as_bool).unwrap_or(false);
    let pool = config::captcha_pool_settings();
    if let Some(object) = body.as_object_mut() {
        object.insert("target".to_string(), Value::from(pool.target as u64));
        object.insert(
            "startPlanAccounts".to_string(),
            Value::from(start_plan_accounts as u64),
        );
        object.insert("demandActive".to_string(), Value::Bool(demand_active));
        object.insert(
            "needsTokens".to_string(),
            Value::Bool(required && demand_active && start_plan_accounts > 0 && ready == 0),
        );
        // 旧客户端继续读取这三个键，但状态轮询不再触发验证。
        object.insert("pollMs".to_string(), Value::from(pool.poll_ms as u64));
        object.insert(
            "maxPerRound".to_string(),
            Value::from(pool.max_per_round as u64),
        );
        // 仅返回公开账号标识，SDK 所需配置仍经账号管理接口读取。
        object.insert(
            "captchaAccountId".to_string(),
            account_id.map(Value::String).unwrap_or(Value::Null),
        );
    }
    body
}

/// 启用的套餐池账号数及代表账号；缺失时退回任意启用的 ZCode 账号。
fn start_plan_accounts(state: &ServerState) -> (usize, Option<String>) {
    let list = state.store().list_accounts();
    let Some(accounts) = list.get("accounts").and_then(Value::as_array) else {
        return (0, None);
    };
    let mut start_plan = 0usize;
    let mut start_plan_id: Option<String> = None;
    let mut fallback_id: Option<String> = None;
    for account in accounts {
        let provider = account.get("provider").and_then(Value::as_str).unwrap_or("");
        if zcode::region::Region::from_provider_id(provider).is_none() {
            continue;
        }
        let id = account.get("id").and_then(Value::as_str).unwrap_or("");
        if account.get("enabled").and_then(Value::as_bool) == Some(false) || id.is_empty() {
            continue;
        }
        if fallback_id.is_none() {
            fallback_id = Some(id.to_string());
        }
        if zcode::plan_of(account) == zcode::PLAN_START {
            start_plan += 1;
            if start_plan_id.is_none() {
                start_plan_id = Some(id.to_string());
            }
        }
    }
    (start_plan, start_plan_id.or(fallback_id))
}

/// GET /api/zcode/captcha —— 池子概况（库存 / 寿命 / 计数 / 是否该补货）
pub async fn get_captcha(State(state): State<ServerState>) -> Response {
    ok_json(stats_json(&state).await)
}

/// POST /api/zcode/captcha —— body `{tokens: [{param, region}]}`；也兼容单条形态。
pub async fn push_captcha(State(state): State<ServerState>, body: Bytes) -> Response {
    if body.len() > verification::MAX_MESSAGE { return management_error(413, "验证码提交超过大小限制"); }
    let payload = match parse_body(&body) {
        Ok(value) => value,
        Err(error) => return management_error(400, error.message),
    };
    let Some(object) = payload.as_object() else {
        return management_error(400, "请求体必须是 JSON 对象");
    };
    // 两种形态归一成一个列表（见上面那条说明）
    let items: Vec<Value> = match object.get("tokens") {
        Some(Value::Array(items)) => items.clone(),
        Some(_) => return management_error(400, "tokens 必须是数组"),
        None => vec![payload.clone()],
    };
    // 独立 WASM 插件的验证码提交必须经主程序签发的短通道：它会用无代理
    // 账号快照核对身份、绑定本次配置，并禁止插件自行读取/写入账号出口。
    let bound_provider = payload["provider"].as_str()
        .filter(|provider| zcode::region::Region::from_provider_id(provider).is_some())
        .filter(|provider| plugin_host::wasm::owns(provider));
    if let Some(provider) = bound_provider {
        let Some(account) = payload["accountId"].as_str().filter(|id| !id.is_empty()) else {
            return management_error(400, "验证码提交必须明确账号");
        };
        let account = match account_provider(&state, account, Some(provider)) {
            Ok(account) => account,
            Err((status, message)) => return management_error(status, message),
        };
        if payload["configId"].as_str().is_none_or(|id| id.trim().is_empty()) {
            return management_error(400, "验证码提交缺少当次配置标识，请重新获取配置");
        }
        let Some(adapter) = adapter::adapter_by_id(provider) else {
            return management_error(503, "验证码执行体不可用");
        };
        let result = adapter.verification_action(state.store(), &account, &payload).await
            .map_err(|error| (error.status_code, error.message));
        let result = match result {
            Ok(result) => result,
            Err((status, message)) => return management_error(status, message),
        };
        let count = items.len() as u64;
        let accepted = result["accepted"].as_u64().filter(|accepted| *accepted <= count)
            .ok_or_else(|| (502, "验证码提交回执无效".to_string()));
        let accepted = match accepted {
            Ok(accepted) => accepted,
            Err((status, message)) => return management_error(status, message),
        };
        if accepted == 0 {
            return management_error(400, "没有可入池的令牌（令牌为空或已提交）");
        }
        let mut response = stats_json(&state).await;
        response["accepted"] = json!(accepted);
        response["provider"] = json!(provider);
        response["accountId"] = json!(account);
        return ok_json(response);
    }
    // 入池的判据（param 非空）也在插件里：网关不解析令牌形态，它只知道
    // 「有一批条目要推过去」。插件回 `accepted`，网关据此决定要不要报 400。
    let accepted = match call_action(&state, "", "captcha.push", &json!({ "tokens": items })).await {
        Ok(outcome) => outcome
            .get("accepted")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        Err((status_code, message)) => return management_error(status_code, message),
    };
    if accepted == 0 {
        return management_error(400, "没有可入池的令牌（令牌为空或已提交）");
    }
    logging::console_line(
        "[ZCode]",
        &format!(
            "🔐 人机验证令牌入池 {} 个（目标 {}）",
            accepted,
            config::captcha_pool_settings().target,
        ),
    );
    let mut response = stats_json(&state).await;
    if let Some(object) = response.as_object_mut() {
        object.insert("accepted".to_string(), Value::from(accepted));
    }
    ok_json(response)
}
