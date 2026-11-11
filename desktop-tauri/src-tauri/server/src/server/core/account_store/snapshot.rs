use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};

use super::state::{AccountState, StoredAccount};
use super::{read_rows, AccountStore};

#[derive(Default)]
pub(super) struct SnapshotCache {
    build: Mutex<()>,
    state: RwLock<Option<CachedSnapshot>>,
}

/// 构建期版本漂移的限频日志（console-only；见 build_routing_snapshot 收尾说明）。
thread_local! {
    static SNAPSHOT_DRIFT_LOG: std::cell::RefCell<Option<Instant>> = const { std::cell::RefCell::new(None) };
}
/// 漂移日志的最小间隔：构建本身高频，逐次打会把 stderr 刷爆。
const DRIFT_LOG_MIN_GAP: Duration = Duration::from_secs(10);

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
            // 等待者：不补偿线程，在当前线程上轮询直到缓存可用。
            // 纯 yield_now 在建者（秒级构建）期间会把 worker 烧成 100%——
            // 2026-11-11 实测五线程 2900+ 秒 CPU 的主力之一。每 64 次 yield
            // 让出 1ms：对同核线程友好，延迟感知不变（构建本来就秒级）。
            let mut spins: u32 = 0;
            loop {
                if let Some(snapshot) = fresh(cache, revision, metadata) { return snapshot; }
                spins = spins.wrapping_add(1);
                if spins % 64 == 0 {
                    std::thread::sleep(Duration::from_millis(1));
                } else {
                    std::thread::yield_now();
                }
            }
        }
    }

    fn build_routing_snapshot(&self, db: &crate::server::db::Db, revision: u64, metadata: u64) -> Arc<RoutingSnapshot> {
        // 剖析插桩（2026-11-12）：增量构建实测 2-6 秒（正常应毫秒级），
        // 把「DB 读密文行」与「解密+投影」分开计时，超过 1 秒打 console。
        let build_started = Instant::now();
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
        let changed_rows = changes.len();
        let db_read_at = build_started.elapsed();
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
        if build_started.elapsed().as_millis() > 1_000 {
            crate::server::logging::console(
                "[Storage]",
                &format!(
                    "路由快照构建 {} ms（DB 读密文行 {} ms，变更 {} 行，全量={}）",
                    build_started.elapsed().as_millis(),
                    db_read_at.as_millis(),
                    changed_rows,
                    replace,
                ),
            );
        }
        let snapshot = Arc::new(RoutingSnapshot::from_rows(&rows));
        // 缓存**无条件写入**（2026-11-11 活锁修复）。
        //
        // 旧实现只在「构建期间版本号没再变」时入缓存，变了就整份丢弃 ——
        // 但构建本身要几秒（大库全量解码），启动期账号写入与目录刷新持续
        // 推进 revision / generation，于是每一轮构建收尾都判定「过期」而
        // 丢弃：下一个等待者从头再建，再被丢弃 —— 无限重建 + 等待者自旋，
        // 五个 worker 全部烧满（实测 CPU 2900+ 秒），运行时饿死。
        //
        // 现在以**构建收尾时刻**的 revision / generation 入缓存：即使构建
        // 期间有变更被本轮漏掉，下一轮构建也会以这份缓存为底、按行级增量
        // 补齐（`previous` 复用 + `account_changes(since)`），一两个增量循环
        // 内必然收敛 —— 慢，但有限，且等待者只自旋一轮。
        //
        // 漂移观测（console-only，不落库）：构建前后版本号不一致时留一行
        // 痕迹 —— 若某写方以极高频率推版本号，这里会连续刷屏，第一现场直指。
        let final_revision = db.account_revision();
        let final_metadata = crate::server::core::providers::catalog::generation();
        if final_revision != revision || final_metadata != metadata {
            SNAPSHOT_DRIFT_LOG.with(|slot| {
                let due = {
                    let last = slot.borrow();
                    last.map(|at| at.elapsed() >= DRIFT_LOG_MIN_GAP).unwrap_or(true)
                };
                // borrow 的守卫先析构，再取 borrow_mut —— 同一 RefCell 嵌套
                // 借用会 panic（release 是 panic=abort，一次就带走整个进程，
                // 2026-11-11 实测 beta.5 首次触发即崩）。
                if due {
                    crate::server::logging::console(
                        "[Storage]",
                        &format!(
                            "路由快照构建期间版本漂移：revision {} → {}，generation {} → {}（本轮结果按收尾值入缓存，下轮增量补齐）",
                            revision, final_revision, metadata, final_metadata,
                        ),
                    );
                    *slot.borrow_mut() = Some(Instant::now());
                }
            });
        }
        if let Ok(mut state) = cache.state.write() {
            *state = Some(CachedSnapshot { revision: final_revision, metadata: final_metadata,
                data_version, checked_at: Instant::now(), rows: Arc::new(rows),
                decoded: Arc::new(decoded), priority_scope, snapshot: snapshot.clone() });
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
        "upstreamCooldownUntil", "zcodePlan", "pool", "quotaChannel", "pluginModelPermissions", "usageConfigKey",
    ] {
        if let Some(value) = account.get(key) { public.insert(key.to_string(), value.clone()); }
    }
    let source = account.pointer("/proxy/config/source").and_then(Value::as_str).unwrap_or("");
    public.insert("proxy".to_string(), json!({ "config": { "source": source } }));
    Value::Object(public)
}
