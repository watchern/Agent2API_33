//! 官网上游版本清单抓取：只服务 `core::upstream_versions` 的定时同步。
//!
//! fork 的更新子系统以 GitHub + jsDelivr 候选源为核心（`client` / `selection`），
//! 与上游 beta.6 整站的 `website` 下载/图标白名单设计不同。这里只保留
//! upstream_versions 实际需要的入口：从官网 CDN/API 拉一份 JSON，带大小与时限，
//! 其余官网 URL 白名单逻辑不在 fork 的信任模型内，故不引入。

use std::time::Duration;

use serde_json::Value;

use crate::server::core::egress;

use super::UpdateError;

/// 官网 CDN 的上游版本清单（优先）。
pub const CDN_UPSTREAM_VERSIONS_URL: &str =
    "https://cdn.agent2api.dev/agent2api/feeds/providers/upstream-versions.json";
/// 官网 API 的上游版本清单（兜底）。
pub const API_UPSTREAM_VERSIONS_URL: &str =
    "https://agent2api.dev/api/providers/upstream-versions";

#[derive(Clone)]
pub struct JsonResponse {
    pub value: Value,
    pub status: reqwest::StatusCode,
}

fn trusted(value: &str) -> bool {
    let Ok(url) = url::Url::parse(value) else { return false };
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return false;
    }
    matches!(
        url.origin().ascii_serialization().as_str(),
        "https://agent2api.dev" | "https://cdn.agent2api.dev"
    )
}

/// 从官网拉 JSON，限制响应体大小与整体超时。
pub async fn fetch_json_limit(value: &str, timeout_ms: u64, limit: usize) -> Result<JsonResponse, UpdateError> {
    if !trusted(value) {
        return Err(UpdateError::new("上游版本同步只允许访问官网"));
    }
    let http = egress::resolved_client(None, false, "", &[]).map_err(UpdateError::new)?;
    let target = url::Url::parse(value).map_err(|_| UpdateError::new("官网上游版本地址无效"))?;
    let headers = [("Accept".to_string(), "application/json".to_string())];
    let mut builder = http.get(target).header(reqwest::header::ACCEPT, "application/json")
        .timeout(Duration::from_millis(timeout_ms));
    for (name, header_value) in &headers {
        builder = builder.header(name.as_str(), header_value.as_str());
    }
    let mut response = tokio::time::timeout(Duration::from_millis(timeout_ms), builder.send())
        .await
        .map_err(|_| UpdateError::new("连接官网上游版本服务超时"))?
        .map_err(|error| UpdateError::new(format!("连接官网上游版本服务失败：{}", egress::describe_error_detail(&error))))?;
    let status = response.status();
    if !status.is_success() {
        return Ok(JsonResponse { value: Value::Null, status });
    }
    if !response.headers().get(reqwest::header::CONTENT_TYPE).and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"))
    {
        return Err(UpdateError::new("官网上游版本信息不是 JSON"));
    }
    if response.content_length().is_some_and(|size| size > limit as u64) {
        return Err(UpdateError::new("官网上游版本信息过大"));
    }
    let bytes = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| UpdateError::new("读取官网上游版本信息失败"))? {
            if bytes.len().saturating_add(chunk.len()) > limit {
                return Err(UpdateError::new("官网上游版本信息过大"));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok::<_, UpdateError>(bytes)
    })
    .await
    .map_err(|_| UpdateError::new("读取官网上游版本信息超时"))??;
    let value = serde_json::from_slice(&bytes).map_err(|_| UpdateError::new("官网上游版本信息格式无效"))?;
    Ok(JsonResponse { value, status })
}
