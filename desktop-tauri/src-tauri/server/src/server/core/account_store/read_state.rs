use std::time::{Duration, Instant};

use super::{read_rows, state::{AccountState, StoredAccount}, AccountStore};

/// 账号直读超过这个耗时就在日志里提醒。正常是毫秒级（小表 + 单行查询）；
/// 超过说明数据库被别的操作占住（**单连接互斥**，见 `db::Db::with` 的说明）
/// 或磁盘 / 杀软在拖 —— 记下耗时分布才能定位。
const SLOW_READ: Duration = Duration::from_millis(500);

impl AccountStore {
    pub(super) fn read_record(&self, id: &str) -> Option<StoredAccount> {
        // 解码缓存跟随 `routing_snapshot()` 的增量更新：命中时不打库。
        // read_record 在 auth 状态（get_status / get_config_summary）与转发链路
        // 上是每请求级的热路径，2026-11-09 起不再每次直读。
        if self.database().is_some() {
            if let Some(record) = self.decoded_record(id) {
                return Some(record);
            }
        }
        let Some(db) = self.database() else { return None };
        let started = Instant::now();
        let raw = {
            let _guard = self.guard();
            db.with(|conn| read_rows::by_id(conn, id)).and_then(Result::ok).flatten()?
        };
        warn_slow_read("按 id 读取账号", started);
        match read_rows::decode(raw) {
            Ok((_, record)) => Some(record),
            Err(error) => {
                super::sql::warn_load_failure("按 id 读取账号", &format!("账号 {id} 读不出来：{}", error.describe()));
                None
            }
        }
    }

    pub(super) fn read_provider(&self, provider: &str) -> Vec<StoredAccount> {
        // 与 read_record 同一取舍：缓存可用就不打库（此路径在转发选路 /
        // 账号视图上高频调用）。缓存不可用时 read_state 内部自己回落直读。
        if self.database().is_none() {
            return Vec::new();
        }
        self.read_state()
            .accounts
            .into_iter()
            .filter(|record| record.provider() == provider)
            .collect()
    }

    pub(super) fn read_state(&self) -> AccountState {
        // 解码缓存：命中时 UI 列表（/api/session 首屏 + 账号页 20 秒轮询）不再
        // 全表读。缓存由 routing_snapshot 的行级变更跟踪维护，写路径推进版本号
        // 即失效并增量重读 —— 语义与选路预演那份一致。
        if self.database().is_some() {
            if let Some(state) = self.decoded_state() {
                return state;
            }
        }
        let Some(db) = self.database() else { return AccountState::default() };
        let started = Instant::now();
        let result = {
            let _guard = self.guard();
            db.with(|conn| {
                Ok::<_, rusqlite::Error>((read_rows::all(conn)?, super::sql::load_priority_scope(conn)?))
            }).and_then(Result::ok)
        };
        warn_slow_read("读取全部账号", started);
        let Some((rows, priority_scope)) = result else { return AccountState::default() };
        let accounts = read_rows::decode_many(rows, "读取全部账号")
            .into_iter().map(|(_, record)| record).collect();
        AccountState { accounts, priority_scope }
    }
}

/// 单次账号读的慢耗时提醒（阈值见 [`SLOW_READ`]）。
fn warn_slow_read(label: &str, started: Instant) {
    let elapsed = started.elapsed();
    if elapsed >= SLOW_READ {
        crate::server::logging::log_with_level(
            "[Storage]",
            &format!(
                "{label} 耗时 {} ms（正常毫秒级；超过说明数据库连接被其它操作占住，或磁盘 / 杀软在拖）",
                elapsed.as_millis()
            ),
            "warn",
        );
    }
}
