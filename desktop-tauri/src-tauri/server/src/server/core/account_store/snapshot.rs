use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use serde_json::{json, Map, Value};

use super::state::{AccountState, StoredAccount};
use super::{read_rows, AccountStore};

#[derive(Default)]
pub(super) struct SnapshotCache {
    build: Mutex<()>,
    state: RwLock<Option<CachedSnapshot>>,
}

struct CachedSnapshot {
    revision: u64,
    metadata: u64,
    data_version: i64,
    checked_at: Instant,
    rows: Arc<BTreeMap<i64, Arc<Value>>>,
    /// 与 `rows` 同一份 rowid 键的**完整解码记录**（`read_rows::decode` 的产物，
    /// 含 extra 合并 —— 选路投影 `rows` 只是它的字段子集）。UI 列表
    /// （`read_state`）与 `read_record` 的热路径读这份，命中时不打库；
    /// 由同一轮增量更新维护，见 `build_routing_snapshot`。
    decoded: Arc<BTreeMap<i64, Arc<StoredAccount>>>,
    priority_scope: Option<String>,
    snapshot: Arc<RoutingSnapshot>,
}

pub(crate) struct RoutingSnapshot {
    pub accounts: Vec<Arc<Value>>,
    pub by_id: HashMap<String, Arc<Value>>,
    pub available_providers: HashSet<String>,
    pub forwarding_providers: HashSet<String>,
}

impl RoutingSnapshot {
    fn from_rows(rows: &BTreeMap<i64, Arc<Value>>) -> Self {
        let accounts: Vec<_> = rows.values().cloned().collect();
        let by_id = accounts.iter().filter_map(|account| {
            Some((account.get("id")?.as_str()?.to_string(), account.clone()))
        }).collect();
        let available_providers = accounts.iter().filter(|account| {
            account.get("enabled").and_then(Value::as_bool) != Some(false)
                && account.get("hasCredentials").and_then(Value::as_bool) == Some(true)
        }).filter_map(|account| account.get("provider")?.as_str().map(str::to_string)).collect();
        let forwarding_providers = accounts.iter().filter_map(|account| {
            account.get("provider")?.as_str().map(str::to_string)
        }).collect::<HashSet<_>>().into_iter()
            .filter(|provider| super::store::provider_forwards_requests(provider)).collect();
        Self { accounts, by_id, available_providers, forwarding_providers }
    }
}

impl AccountStore {
    /// 路由快照：**双检锁**，避免 block_in_place 补偿线程雪崩。
    ///
    /// 第一检：无锁读 `fresh()`（版本号 + metadata 命中即返回）。
    /// 未命中时取 `build.lock`（建者锁），锁内第二检（leader 可能已建好）。
    /// 只有**持锁的建者**调 `block_in_place`；等待者在锁外 `yield_now` 轮询，
    /// 不补偿线程 —— 2026-11-09 冻结根因：旧实现 N 个并发请求各自 block_in_place
    /// → N 个补偿线程争 build.lock → 进程堆锁雪崩。
    pub(crate) fn routing_snapshot(&self) -> Arc<RoutingSnapshot> {
        let Some(db) = self.database() else { return empty() };
        let cache = self.snapshot_cache();
        let revision = db.account_revision();
        let metadata = crate::server::core::providers::catalog::generation();
        if let Some(snapshot) = fresh(cache, revision, metadata) { return snapshot; }
        // 只有一个请求能拿到 build.lock（建者），其余在锁外轮询
        if let Ok(_guard) = cache.build.try_lock() {
            // 锁内第二检：可能已被前一个建者建好
            if let Some(snapshot) = fresh(cache, revision, metadata) { return snapshot; }
            crate::server::core::blocking::in_place(|| self.build_routing_snapshot(db, revision, metadata))
        } else {
            // 等待者：不补偿线程，在当前线程上 yield 轮询直到缓存可用
            loop {
                if let Some(snapshot) = fresh(cache, revision, metadata) { return snapshot; }
                std::thread::yield_now();
            }
        }
    }

    fn build_routing_snapshot(&self, db: &crate::server::db::Db, revision: u64, metadata: u64) -> Arc<RoutingSnapshot> {
        let cache = self.snapshot_cache();
        // build.lock 已由调用方（routing_snapshot）持有 try_lock，这里不再重复取。
        // 锁内第二检也已在调用方完成，这里直接开始构建。
        let previous = cache.state.read().ok().and_then(|state| state.as_ref().map(|cached| {
            (cached.revision, cached.metadata, cached.data_version, cached.rows.clone(), cached.decoded.clone())
        }));
        // 连接锁只复制密文行；解密、规则推导和轻量投影全部在锁外执行。
        let batch = db.with(|conn| -> rusqlite::Result<_> {
            let data_version: i64 = conn.pragma_query_value(None, "data_version", |row| row.get(0))?;
            // 队列作用域与账号数据同库同版本号：随这一轮增量一并刷新
            let priority_scope = super::sql::load_priority_scope(conn).ok().flatten();
            let since = previous.as_ref().filter(|(_, old_metadata, old_version, _, _)| {
                *old_metadata == metadata && *old_version == data_version
            }).map(|(revision, _, _, _, _)| *revision);
            let changes = db.account_changes(since);
            let (replace, rows, unchanged) = match changes.rows {
                Some(ids) => {
                    let unchanged = ids.is_empty();
                    (false, read_rows::changed(conn, &ids)?, unchanged)
                }
                None => (true, read_rows::all(conn)?.into_iter().map(|row| (row.rowid, Some(row))).collect(), false),
            };
            Ok((changes.revision, data_version, priority_scope, replace, rows, unchanged))
        }).and_then(Result::ok);
        let Some((revision, data_version, priority_scope, replace, changes, unchanged)) = batch else { return empty() };
        if unchanged && db.account_revision() == revision
            && crate::server::core::providers::catalog::generation() == metadata {
            if let Ok(mut state) = cache.state.write() {
                if let Some(cached) = state.as_mut() {
                    cached.checked_at = Instant::now();
                    return cached.snapshot.clone();
                }
            }
        }
        let mut rows = if replace { BTreeMap::new() } else {
            previous.as_ref().map(|(_, _, _, rows, _)| (**rows).clone()).unwrap_or_default()
        };
        let mut decoded = if replace { BTreeMap::new() } else {
            previous.as_ref().map(|(_, _, _, _, decoded)| (**decoded).clone()).unwrap_or_default()
        };
        let mut failed = 0;
        for (rowid, raw) in changes {
            rows.remove(&rowid);
            decoded.remove(&rowid);
            if let Some(raw) = raw {
                match read_rows::decode(raw) {
                    Ok((_, record)) => {
                        decoded.insert(rowid, Arc::new(record.clone()));
                        rows.insert(rowid, Arc::new(route_account(&self.public_account(&record))));
                    }
                    Err(_) => failed += 1,
                }
            }
        }
        if failed > 0 {
            super::sql::warn_load_failure("读取选路账号", &format!("{failed} 条账号记录解密或解析失败，已跳过；原始数据未修改"));
        }
        let snapshot = Arc::new(RoutingSnapshot::from_rows(&rows));
        if db.account_revision() == revision && crate::server::core::providers::catalog::generation() == metadata {
            if let Ok(mut state) = cache.state.write() {
                *state = Some(CachedSnapshot { revision, metadata, data_version,
                    checked_at: Instant::now(), rows: Arc::new(rows),
                    decoded: Arc::new(decoded), priority_scope, snapshot: snapshot.clone() });
            }
        }
        snapshot
    }

    /// 解码态缓存：先调 `routing_snapshot()`（负责过期校验与行级增量更新），
    /// 再从同一缓存取**完整解码记录**。命中时不打库 —— `/api/session`（账号
    /// 列表）、`read_record`（auth 状态 / 转发链路）都是每请求级的热路径，
    /// 2026-11-09 起不再每次全表读。None = 缓存不可用（库未打开 / 锁中毒）。
    pub(super) fn decoded_state(&self) -> Option<AccountState> {
        self.routing_snapshot();
        let cache = self.snapshot_cache();
        let state = cache.state.read().ok()?;
        let cached = state.as_ref()?;
        let accounts = cached.decoded.values().map(|record| (**record).clone()).collect();
        Some(AccountState { accounts, priority_scope: cached.priority_scope.clone() })
    }

    pub(super) fn decoded_record(&self, id: &str) -> Option<StoredAccount> {
        self.routing_snapshot();
        let cache = self.snapshot_cache();
        let state = cache.state.read().ok()?;
        let cached = state.as_ref()?;
        cached.decoded.values().find(|record| record.id() == id).map(|record| (**record).clone())
    }

    pub(crate) fn route_account_ids_for_provider(&self, provider: &str) -> Vec<String> {
        let snapshot = self.routing_snapshot();
        let mut accounts: Vec<_> = snapshot.accounts.iter().filter(|account| {
            account.get("provider").and_then(Value::as_str) == Some(provider)
                && account.get("enabled").and_then(Value::as_bool) != Some(false)
                && account.get("hasCredentials").and_then(Value::as_bool) == Some(true)
        }).collect();
        accounts.sort_by_key(|account| super::store::route_order_key(account));
        accounts.into_iter().filter_map(|account| account.get("id")?.as_str().map(str::to_string)).collect()
    }

    pub(crate) fn route_account(&self, id: &str) -> Option<Arc<Value>> {
        self.routing_snapshot().by_id.get(id).cloned()
    }
}

fn fresh(cache: &SnapshotCache, revision: u64, metadata: u64) -> Option<Arc<RoutingSnapshot>> {
    let state = cache.state.read().ok()?;
    let cached = state.as_ref()?;
    // **无 TTL**：版本号 + 目录 generation 不变即有效。写路径推进 account_revision
    // 或目录 generation 时自然失效，不需要时间兜底。
    //
    // 旧实现用 `checked_at.elapsed() < 1s` —— 每秒过期一次，过期后的首个请求
    // 走 block_in_place 重建，而同一秒内并发的 N 个请求各自都走 block_in_place
    // （补偿线程）。账号页首屏的 N 个读同时撞上「缓存刚过期」的瞬间，就会
    // 生成 N 个补偿线程争 build.lock —— 2026-11-09 实测冻结实例的 44 个
    // tokio worker（默认 4 + 40 补偿）就是这么来的。
    (cached.revision == revision && cached.metadata == metadata).then(|| cached.snapshot.clone())
}

fn empty() -> Arc<RoutingSnapshot> {
    Arc::new(RoutingSnapshot::from_rows(&BTreeMap::new()))
}

// 复用各家公开投影的字段口径，只缓存选路和发送编排需要的部分。
fn route_account(account: &Value) -> Value {
    let mut public = Map::new();
    for key in [
        "id", "provider", "name", "nickname", "email", "userId", "uid", "account",
        "displayName", "priority", "addedAt", "enabled", "hasCredentials", "nameCustom",
        "needsRelogin", "allowRouteSkip", "maxConcurrent", "rateLimits", "limiterGroups",
        "upstreamCooldownUntil", "zcodePlan", "pool", "quotaChannel",
    ] {
        if let Some(value) = account.get(key) { public.insert(key.to_string(), value.clone()); }
    }
    let source = account.pointer("/proxy/config/source").and_then(Value::as_str).unwrap_or("");
    public.insert("proxy".to_string(), json!({ "config": { "source": source } }));
    Value::Object(public)
}
