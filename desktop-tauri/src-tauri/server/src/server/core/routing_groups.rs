//! 跨模型成员投影。运行时字段只存在于候选副本，不写入账号库。

use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use crate::server::config::{self, RoutingGroup};
use super::{account_store::AccountStore, capability, custom_providers, key_scope::{self, KeyScope}, models::model_id, providers, routing};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestNeeds {
    #[serde(default)] pub tools: bool,
    #[serde(default)] pub images: bool,
    #[serde(default)] pub video: bool,
}

impl RequestNeeds {
    pub fn from_body(body: &Value) -> Self {
        let mut needs = Self {
            tools: ["tools", "functions"].iter().any(|key| body.get(key).and_then(Value::as_array).is_some_and(|tools| !tools.is_empty())),
            ..Default::default()
        };
        for message in body.get("messages").and_then(Value::as_array).into_iter().flatten() {
            needs.tools |= matches!(message.get("role").and_then(Value::as_str), Some("tool" | "function"))
                || message.get("function_call").is_some()
                || message.get("tool_calls").and_then(Value::as_array).is_some_and(|calls| !calls.is_empty());
            for part in message.get("content").and_then(Value::as_array).into_iter().flatten() {
                match part.get("type").and_then(Value::as_str) {
                    Some("image_url" | "input_image" | "image") => needs.images = true,
                    Some("video_url" | "input_video" | "video") => needs.video = true,
                    _ => {},
                }
            }
        }
        needs
    }

    pub fn reason(&self, item: &Value) -> Option<String> {
        for (required, key, label) in [(self.tools, "supportsToolCall", "工具调用"),
            (self.images, "supportsImages", "图片输入"), (self.video, "supportsVideo", "视频输入")] {
            if required && item.get(key).and_then(Value::as_bool) != Some(true) {
                return Some(if item.get(key).and_then(Value::as_bool) == Some(false) {
                    format!("该模型不支持{label}，已跳过")
                } else { format!("该模型的{label}能力未声明，请先在模型管理中确认") });
            }
        }
        None
    }
}

fn manifest(provider: &str) -> Vec<Value> {
    if let Some(owner) = custom_providers::get(provider) {
        owner.get("models").and_then(Value::as_array).cloned().unwrap_or_default().into_iter().map(|mut item| {
            super::model_meta::apply(&mut item);
            let overrides = capability::normalize_object(item.get("capabilities"));
            capability::apply_overrides(&mut item, &overrides);
            item
        }).collect()
    } else { providers::catalog::manifest_for_provider(provider) }
}

pub fn catalog_model(provider: &str, model: &str) -> Option<Value> {
    manifest(provider).into_iter().find(|item| model_id(item).eq_ignore_ascii_case(model))
}

pub fn choices(store: &AccountStore) -> Vec<Value> {
    let accounts = store.routing_snapshot();
    let mut ids: Vec<_> = accounts.accounts.iter().map(|account| routing::provider_of(account).to_string()).collect();
    ids.extend(providers::executable_provider_ids());
    ids.extend(custom_providers::list().iter().filter_map(|provider| provider.get("id").and_then(Value::as_str).map(str::to_string)));
    ids.sort(); ids.dedup();
    let mut result = Vec::new();
    for provider in ids {
        if !super::account_store::store_view::account_visible(&provider) { continue; }
        let models = manifest(&provider);
        let mut seen = HashSet::new();
        for item in models {
            let model = model_id(&item);
            if model.is_empty() || !seen.insert(model.to_ascii_lowercase()) { continue; }
            if providers::images::model_is_image(&item) { continue; }
            result.push(json!({ "provider": provider, "providerLabel": providers::label_of(&provider), "model": model,
                "name": item.get("name").and_then(Value::as_str).unwrap_or(&model), "capabilities": capability::effective(&item),
                "accountCount": accounts.accounts.iter().filter(|account| routing::provider_of(account) == provider).count() }));
        }
    }
    result
}

pub fn candidate_key(member: &str, account: &str) -> String { format!("{member}\0{account}") }
pub fn account_key(key: &str) -> &str { key.split_once('\0').map(|(_, account)| account).unwrap_or(key) }
pub fn member_key(key: &str) -> Option<&str> { key.split_once('\0').map(|(member, _)| member) }
pub fn attempt_key(account: &Value) -> String {
    account.get("routingCandidateId").and_then(Value::as_str).unwrap_or_else(|| routing::account_id(account).unwrap_or("")).to_string()
}
pub fn candidate_model(account: &Value) -> Option<&str> { account.get("routingModel").and_then(Value::as_str) }
pub fn family_key(account: &Value) -> &str {
    account.get("routingMemberId").and_then(Value::as_str).unwrap_or_else(|| routing::provider_of(account))
}

#[derive(Clone, Default)]
pub struct GroupProjection {
    pub accounts: Vec<Value>,
    pub blocked: Vec<Value>,
}

fn provider_active(provider: &str) -> bool {
    super::account_store::store_view::account_visible(provider)
        && custom_providers::get(provider).map_or_else(|| providers::adapter::adapter_by_id(provider).is_some(), |owner| owner.get("enabled") != Some(&Value::Bool(false)))
}

pub fn project(group: &RoutingGroup, accounts: &[Value], needs: &RequestNeeds, scope: Option<&KeyScope>) -> GroupProjection {
    project_with_vision(group, accounts, needs, scope, None)
}

pub fn project_with_vision(group: &RoutingGroup, accounts: &[Value], needs: &RequestNeeds, scope: Option<&KeyScope>,
    vision: Option<&config::vision_compat::VisionSettings>) -> GroupProjection {
    let started = std::time::Instant::now();
    let mut result = GroupProjection::default();
    let by_id: HashMap<_, _> = accounts.iter().filter_map(|account| routing::account_id(account).map(|id| (id, account))).collect();
    // ── 按家预计算（2026-11-14 beta-0 插桩结论）──────────────────────
    // 成员数（本机 36）远多于家数（约 5），而清单、可用性、展示名都只跟
    // provider 有关：原来每遇到一个成员就各算一次，账号投影段因此随候选
    // 数线性涨到 3-8 秒。这里一轮只算「去重后的家数」次，成员循环纯查表。
    // 三个查询都没有副作用，多算几家（含未 included 的成员）不改变结果。
    let ids: HashSet<&str> = group.members.iter().map(|member| member.provider.as_str()).collect();
    let mut manifests: HashMap<String, Vec<Value>> = HashMap::new();
    for id in &ids { manifests.insert((*id).to_string(), manifest(id)); }
    let manifest_at = started.elapsed();
    let mut active: HashMap<&str, bool> = HashMap::new();
    for id in &ids { active.insert(*id, provider_active(id)); }
    let active_at = started.elapsed();
    let mut labels: HashMap<&str, String> = HashMap::new();
    for id in &ids { labels.insert(*id, providers::label_of(id)); }
    let labels_at = started.elapsed();
    for member in group.members.iter().filter(|member| member.included) {
        let item = manifests.get(&member.provider).and_then(|items| items.iter().find(|item| model_id(item).eq_ignore_ascii_case(&member.model))).cloned();
        let reason = if !key_scope::allows_provider(scope, &member.provider) { Some("网关 Key 不允许此提供商".to_string()) }
            else if !active.get(member.provider.as_str()).copied().unwrap_or(false) { Some("提供商当前不可用".to_string()) }
            else if let Some(item) = &item {
                if providers::images::model_is_image(item) { Some("生图模型不能用于对话路由".to_string()) }
                else {
                    let mut requirements = needs.clone();
                    if vision.is_some_and(|settings| settings.mode_for(&member.provider, &member.model).is_some()) {
                        requirements.images = false;
                    }
                    requirements.reason(item)
                }
            } else { Some("模型已不在提供商目录中，配置保留但不参与转发".to_string()) };
        let actual = item.as_ref().map(model_id).unwrap_or_else(|| member.model.clone());
        if let Some(reason) = reason {
            result.blocked.push(json!({ "id": member.id, "memberId": member.id, "provider": member.provider,
                "providerLabel": labels.get(member.provider.as_str()).cloned().unwrap_or_else(|| member.provider.clone()),
                "model": actual, "name": actual, "state": "unavailable", "reason": reason }));
            continue;
        }
        if member.accounts.is_empty() {
            result.blocked.push(json!({ "id": member.id, "memberId": member.id, "provider": member.provider, "model": actual,
                "name": actual, "state": "excluded", "reason": "该成员没有参与账号" }));
        }
        for id in &member.accounts {
            let Some(account) = by_id.get(id.as_str()).filter(|account| routing::provider_of(account) == member.provider) else {
                result.blocked.push(json!({ "id": id, "memberId": member.id, "provider": member.provider, "model": actual,
                    "name": id, "state": "unavailable", "reason": "参与账号已移除或不属于此提供商" }));
                continue;
            };
            let mut account = (*account).clone();
            account["routingCandidateId"] = candidate_key(&member.id, id).into();
            account["routingMemberId"] = member.id.clone().into();
            account["routingModel"] = actual.clone().into();
            account["routingPriority"] = result.accounts.len().into();
            result.accounts.push(account);
        }
    }
    let total = started.elapsed();
    if total.as_millis() > 1_000 {
        // 四个数都是**累计**时刻，相邻相减即该段耗时；>1 秒才打，正常请求零开销。
        crate::server::logging::console(
            "[Routing]",
            &format!(
                "路由组投影 {} ms（累计：清单准备 {} / 可用性 {} / 展示名 {} / 成员循环 {} ms，成员 {} 家 {}）",
                total.as_millis(), manifest_at.as_millis(), active_at.as_millis(),
                labels_at.as_millis(), total.as_millis(), group.members.len(), ids.len(),
            ),
        );
    }
    result
}

pub fn validate_members(group: &RoutingGroup, old: Option<&RoutingGroup>, accounts: &[Value]) -> Result<(), String> {
    for member in &group.members {
        let previous = old.and_then(|group| group.members.iter().find(|item| item.id == member.id && item.provider == member.provider && item.model == member.model));
        match catalog_model(&member.provider, &member.model) {
            Some(item) if providers::images::model_is_image(&item) => return Err("对话路由组不能加入生图模型".into()),
            None if previous.is_none() => return Err(format!("提供商中找不到模型 {}，请刷新后重试", member.model)),
            _ => {},
        }
        for id in &member.accounts {
            if let Some(account) = accounts.iter().find(|account| routing::account_id(account) == Some(id)) {
                if routing::provider_of(account) != member.provider { return Err("参与账号不属于所选提供商".into()); }
            } else if !previous.is_some_and(|item| item.accounts.contains(id)) { return Err("参与账号不存在，请刷新后重试".into()); }
        }
    }
    Ok(())
}

pub fn append_public(store: &AccountStore, scope: Option<&KeyScope>, data: &mut Vec<Value>) {
    let accounts = store.routing_snapshot();
    // 一家的清单在本轮只建一次：catalog_model 逐成员调 manifest(provider)，
    // 而清单构建（list_models + 元数据 + 能力覆写）在大家数 × 大清单下是
    // 本轮目录构建里最重的一段（2026-11-12 剖析插桩的假定热点）。
    let mut manifests: std::collections::HashMap<String, Vec<Value>> = std::collections::HashMap::new();
    // 可用性同样只跟 provider 有关：一个组里 36 个成员常只有 5 家，
    // 逐成员调 adapter_by_id 会重复几十次（与 project_with_vision 同因）。
    let mut active: std::collections::HashMap<String, bool> = std::collections::HashMap::new();
    for group in config::routing_profiles().groups.values() {
        if !key_scope::allows_model(scope, &group.model) || data.iter().any(|item| model_id(item).eq_ignore_ascii_case(&group.model)) { continue; }
        let members: Vec<_> = group.members.iter().filter(|member| member.included && key_scope::allows_provider(scope, &member.provider)
            && *active.entry(member.provider.clone()).or_insert_with(|| provider_active(&member.provider))
            && accounts.available_providers.contains(&member.provider) && member.accounts.iter().any(|id|
                accounts.accounts.iter().any(|account| routing::account_id(account) == Some(id) && routing::provider_of(account) == member.provider && account.get("enabled") != Some(&Value::Bool(false)))))
            .filter_map(|member| {
                let items = manifests.entry(member.provider.clone()).or_insert_with(|| manifest(&member.provider));
                items.iter().find(|item| model_id(item).eq_ignore_ascii_case(&member.model)).cloned()
            }).filter(|item| !providers::images::model_is_image(item)).collect();
        if members.is_empty() { continue; }
        let mut item = json!({ "id": group.model, "name": group.name, "object": "model", "owned_by": "routing-group", "kind": "chat", "routing_group": true });
        for (key, public) in [("supportsToolCall", "supports_tool_call"), ("supportsImages", "supports_images"),
            ("supportsVideo", "supports_video"), ("supportsReasoning", "supports_reasoning")] {
            item[public] = members.iter().all(|item| item.get(key).and_then(Value::as_bool) == Some(true)).into();
        }
        for (key, public) in [("maxInputTokens", "max_input_tokens"), ("maxOutputTokens", "max_output_tokens")] {
            if members.iter().all(|item| item.get(key).and_then(Value::as_u64).is_some()) {
                item[public] = members.iter().filter_map(|item| item.get(key).and_then(Value::as_u64)).min().map(Value::from).unwrap_or(Value::Null);
            }
        }
        let mut modalities = vec!["text"];
        if item["supports_images"] == true { modalities.push("image"); }
        if item["supports_video"] == true { modalities.push("video"); }
        item["input_modalities"] = json!(modalities);
        item["created"] = 0.into();
        item["is_default"] = false.into();
        data.push(item);
    }
}
