use serde_json::Value;

use super::state::StoredAccount;
use crate::server::core::beijing;

pub(super) const LEDGER_KIND: &str = "dailyCheckin";
const LEDGER_KEY: &str = "pluginLedger.dailyCheckin";

/// ZCode 系没有签到接口：它每天的「活动」其实是限时套餐领取。
/// 用户口径（2026-11-14）：**当天领取成功就视为今日已签**。
///
/// 判据读账号上的 `claimAt` —— `mark_zcode_claim` 只在领取成功或上游回
/// 「已领过」时写它，按北京自然日比对。手动领取面板只回领取结果，插件未必
/// 同时上报 `dailyCheckin` 的 `completed` 标记，所以不能只靠那条台账。
/// 这里**不写** `checkinAt`：签到链与领取链各写各的字段，只是判「今天做没做」
/// 时把领取也认进来，避免两处状态互相覆盖。
fn zcode_claimed_today(provider: &str, claim_at: i64) -> bool {
    super::is_zcode_family(provider)
        && claim_at > 0
        && beijing::date_key_of_ms(claim_at)
            .is_some_and(|date| date == beijing::today_key())
}

pub(super) fn record_status(record: &StoredAccount) -> Option<&Value> {
    record.get(LEDGER_KEY).filter(|value| {
        (rolling_until(value).is_some() || value.get("at").and_then(Value::as_i64)
            .and_then(beijing::date_key_of_ms)
            .is_some_and(|date| date == beijing::today_key()))
            && value.get("completed").and_then(Value::as_bool).is_some()
    })
}

pub(crate) fn rolling_until(value: &Value) -> Option<i64> {
    if value.get("kind").and_then(Value::as_str) != Some("rolling") { return None; }
    value.get("until").and_then(Value::as_i64).filter(|at| *at > 0 && beijing::date_key_of_ms(*at).is_some())
}

pub(super) fn completed(record: &StoredAccount) -> bool {
    // ZCode 的「签到」就是当日活动领取：领取成功优先于 dailyCheckin 台账，
    // 否则手动领了套餐之后，台账仍停在未完成，签到中心会一直显示「待签」。
    if zcode_claimed_today(record.provider().as_str(), record.claim_at()) {
        return true;
    }
    if let Some(status) = record_status(record) {
        if let Some(until) = rolling_until(status) {
            return until > crate::server::logging::now_ms() && status.get("completed").and_then(Value::as_bool) == Some(true);
        }
    }
    record_status(record)
        .and_then(|value| value.get("completed"))
        .and_then(Value::as_bool)
        .unwrap_or_else(|| {
            record.checkin_at() > 0 && beijing::date_key_of_ms(record.checkin_at())
                .is_some_and(|date| date == beijing::today_key())
        })
}

pub(super) fn apply(record: &mut StoredAccount, value: &Value) -> Result<(), String> {
    if value.is_null() { return Ok(()); }
    let at = value.get("at").and_then(Value::as_i64)
        .filter(|at| *at > 0 && beijing::date_key_of_ms(*at).is_some())
        .ok_or_else(|| "每日签到状态缺少有效时间戳".to_string())?;
    let completed = value.get("completed").and_then(Value::as_bool)
        .ok_or_else(|| "每日签到状态缺少完成标记".to_string())?;
    if value.get("kind").and_then(Value::as_str) == Some("rolling")
        && !rolling_until(value).is_some_and(|until| until > at && until <= at + 366 * 86_400_000)
    {
        return Err("滚动签到状态缺少有效的下次领取时间".into());
    }
    let count = value.get("completedCount").and_then(Value::as_u64);
    let total = value.get("totalCount").and_then(Value::as_u64);
    if count.zip(total).is_some_and(|(count, total)| count > total)
        || completed && (!count.zip(total).is_some_and(|(count, total)| total > 0 && count == total)
            || value.get("sourcesComplete").and_then(Value::as_bool) != Some(true))
    {
        return Err("每日签到状态的领取数量或来源状态不一致".to_string());
    }
    if let Some(status) = value.get("status") {
        let status = status.as_str().filter(|status| matches!(*status,
            "claimed" | "done" | "confirming" | "inactive" | "ineligible" | "captcha" | "failed"))
            .ok_or_else(|| "每日签到状态类型无效".to_string())?;
        if completed != matches!(status, "claimed" | "done") {
            return Err("每日签到状态与完成标记不一致".to_string());
        }
    }
    if value.get("message").is_some_and(|message| !message.is_string()
        || message.as_str().is_some_and(|message| message.chars().count() > 2000))
        || value.get("queryOnly").is_some_and(|query| !query.is_boolean())
        || value.get("retryAt").is_some_and(|retry| !retry.is_null()
            && !retry.as_i64().is_some_and(|retry| retry > 0 && beijing::date_key_of_ms(retry).is_some()))
    {
        return Err("每日签到状态的说明或查询时间无效".to_string());
    }
    if completed && at >= record.checkin_at() {
        record.set_checkin_at(at);
    }
    Ok(())
}

pub(crate) fn checked_in_today(account: &Value) -> bool {
    // 与 `completed` 同一口径：公开形态里的 `checkedInToday` 可能由旧数据注入
    // 为 false，ZCode 当日领取要优先于它。签到中心、排程、重试都走这里。
    if zcode_claimed_today(
        account.get("provider").and_then(Value::as_str).unwrap_or(""),
        account.get("claimAt").and_then(Value::as_i64).unwrap_or(0),
    ) {
        return true;
    }
    account.get("checkedInToday").and_then(Value::as_bool).unwrap_or_else(|| {
        account.get("checkinAt").and_then(Value::as_i64)
            .filter(|at| *at > 0)
            .and_then(beijing::date_key_of_ms)
            .is_some_and(|date| date == beijing::today_key())
    })
}
