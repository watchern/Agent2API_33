use super::*;

fn version(conn: &Connection) -> i64 {
    conn.pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap()
}

fn table_names(conn: &Connection) -> Vec<String> {
    conn.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn has_column(conn: &Connection, table: &str, column: &str) -> bool {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    let mut rows = statement.query([]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        let name: String = row.get(1).unwrap();
        if name == column {
            return true;
        }
    }
    false
}

/// 从**本 fork 旧版**（v30 是「logs 索引」）升上来的库，必须补上上游 v30 的
/// `account_usage_records.diagnostics` 列 —— 缺了它，`core::usage_records` 的
/// `RECORD_COLUMNS` 会让每一次余额查询报「no such column: diagnostics」。
///
/// 这条用例是 v30 撞号的直接回归测试：v30 在两条谱系里含义不同，而迁移按
/// `user_version` 逐版本推进、每个版本只跑一次 ⇒ 停在 30 的库永远不会执行
/// 上游 v30 的那条 `ALTER TABLE`。v37 的幂等补列是唯一的补救路径。
#[test]
fn fork_v30_database_gains_the_upstream_diagnostics_column() {
    let conn = Connection::open_in_memory().unwrap();
    let mut cleaned = 0;
    // 复刻 fork 旧版的库形态：v1..v29 照跑，v30 跑的是 **fork 那一版**
    // （logs 索引），因此 diagnostics 列不在库里。
    for version in 1..=29 {
        apply_version(&conn, version, &mut cleaned).unwrap();
    }
    conn.execute_batch(include_str!("schema_logs_level.sql")).unwrap();
    conn.pragma_update(None, "user_version", 30).unwrap();

    assert!(
        !has_column(&conn, "account_usage_records", "diagnostics"),
        "前置条件：fork 旧版的库里不应有 diagnostics 列"
    );

    migrate(&conn).unwrap();

    assert_eq!(version(&conn), SCHEMA_VERSION);
    assert!(
        has_column(&conn, "account_usage_records", "diagnostics"),
        "从 fork 旧版（user_version=30）升级后必须补上 diagnostics 列"
    );
}

/// 从 fork 旧版（v33=logs 索引、v34=补列，user_version 已到 34）升上来的库，
/// 必须补回上游 v33 的 task_plugins/task_jobs/task_job_runs 三表、v34 的
/// requests.privacy_hits 列。缺了它们，定时任务插件每 5 秒报
/// `no such table: task_jobs`，请求统计报 `no such column: privacy_hits`。
///
/// 复刻方式：1..=32 照上游跑，33/34 跑 fork 旧谱系的内容（logs 索引 + 补列），
/// 35..37 照上游跑 —— 这样 user_version=37 且恰好缺 v33/v34 的对象。
#[test]
fn fork_v34_database_gains_the_upstream_task_tables_and_privacy_column() {
    let conn = Connection::open_in_memory().unwrap();
    let mut cleaned = 0;
    for version in 1..=32 {
        apply_version(&conn, version, &mut cleaned).unwrap();
    }
    // fork 旧谱系的 v33/v34：logs 索引 + 补列，而不是上游的任务表 / 隐私列。
    conn.execute_batch(include_str!("schema_logs_level.sql"))
        .unwrap();
    ensure_columns_34(&conn).unwrap();
    for version in 35..=37 {
        apply_version(&conn, version, &mut cleaned).unwrap();
    }
    conn.pragma_update(None, "user_version", 37).unwrap();

    let tables = table_names(&conn);
    assert!(!tables.iter().any(|t| t == "task_jobs"), "前置条件：fork 旧库不应有 task_jobs 表");
    assert!(!has_column(&conn, "requests", "privacy_hits"), "前置条件：fork 旧库不应有 privacy_hits 列");
    // v35 的本机直连表在 fork 旧谱系里没被占号，应当已经跑到。
    assert!(tables.iter().any(|t| t == "local_direct_refs"), "前置条件：v35 本机直连表应已存在");

    migrate(&conn).unwrap();

    assert_eq!(version(&conn), SCHEMA_VERSION);
    let tables = table_names(&conn);
    for table in ["task_plugins", "task_jobs", "task_job_runs"] {
        assert!(tables.iter().any(|t| t == table), "升级后必须有 {table} 表");
    }
    assert!(has_column(&conn, "requests", "privacy_hits"), "升级后必须有 privacy_hits 列");
    // 已存在的对象不能被重复 ALTER 破坏。
    assert!(has_column(&conn, "requests", "origin"), "已有的 origin 列应保留");
    assert!(tables.iter().any(|t| t == "local_direct_refs"), "已有的本机直连表应保留");
}

/// v38 的补偿对全新库 / 上游谱系的库是空操作（对象都已在，不能因重复建表或
/// "duplicate column name" 失败）。
#[test]
fn upstream_backfill_is_idempotent_for_databases_that_already_have_everything() {
    let conn = Connection::open_in_memory().unwrap();
    let mut cleaned = 0;
    for version in 1..=SCHEMA_VERSION {
        apply_version(&conn, version, &mut cleaned).unwrap();
    }
    conn.pragma_update(None, "user_version", SCHEMA_VERSION)
        .unwrap();

    let tables = table_names(&conn);
    assert!(tables.iter().any(|t| t == "task_jobs"));
    assert!(has_column(&conn, "requests", "privacy_hits"));
    assert!(has_column(&conn, "requests", "origin"));

    // 直接再跑一次补偿函数（不经版本号），必须不报错、不新增对象。
    ensure_upstream_v33_v35(&conn).unwrap();
    assert_eq!(table_names(&conn), tables);
    migrate(&conn).unwrap();
    assert_eq!(version(&conn), SCHEMA_VERSION);
}

/// v37 的补列对**已经有这些列**的库是空操作（上游谱系 / 全新库都走这条路）。
#[test]
fn column_backfill_is_idempotent_for_databases_that_already_have_the_columns() {
    let conn = Connection::open_in_memory().unwrap();
    let mut cleaned = 0;
    for version in 1..=SCHEMA_VERSION {
        apply_version(&conn, version, &mut cleaned).unwrap();
    }
    conn.pragma_update(None, "user_version", SCHEMA_VERSION).unwrap();

    assert!(has_column(&conn, "account_usage_records", "diagnostics"));
    assert!(has_column(&conn, "requests", "cost_usd"));
    assert!(has_column(&conn, "request_daily", "unpriced_requests"));

    // 再跑一遍必须是空操作，不能因「duplicate column name」失败。
    migrate(&conn).unwrap();
    assert_eq!(version(&conn), SCHEMA_VERSION);
}

#[test]
fn every_supported_schema_version_can_upgrade_without_losing_accounts() {
    for old in 1..=SCHEMA_VERSION {
        let conn = Connection::open_in_memory().unwrap();
        let mut cleaned = 0;
        for version in 1..=old {
            apply_version(&conn, version, &mut cleaned).unwrap();
        }
        conn.pragma_update(None, "user_version", old).unwrap();
        let data = r#"{"id":"kept","accessToken":"secret","future":{"keep":true}}"#;
        conn.execute(
            "INSERT INTO accounts(id,provider,priority,enabled,added_at,data) VALUES ('kept','workbuddy',7,1,0,?1)",
            [data],
        ).unwrap();
        migrate(&conn).unwrap();
        assert_eq!(version(&conn), SCHEMA_VERSION, "from={old}");
        let stored: String = conn
            .query_row("SELECT data FROM accounts WHERE id='kept'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(stored, data, "from={old}");
        let tables = table_names(&conn);
        migrate(&conn).unwrap();
        assert_eq!(table_names(&conn), tables, "二次升级应为空操作：from={old}");
    }
}

#[test]
fn newer_database_is_rejected_before_any_schema_or_data_change() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE future_data(value TEXT); INSERT INTO future_data VALUES ('keep');",
    )
    .unwrap();
    conn.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
        .unwrap();
    let before = table_names(&conn);
    assert!(migrate(&conn).is_err());
    assert_eq!(version(&conn), SCHEMA_VERSION + 1);
    assert_eq!(table_names(&conn), before);
    assert_eq!(
        conn.query_row("SELECT value FROM future_data", [], |row| row
            .get::<_, String>(0))
            .unwrap(),
        "keep"
    );
}

#[test]
fn migration_failure_does_not_advance_the_version_and_can_be_retried() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(V1_SCHEMA).unwrap();
    conn.pragma_update(None, "user_version", 1).unwrap();
    conn.execute_batch("ALTER TABLE requests RENAME TO held_requests;")
        .unwrap();
    assert!(migrate(&conn).is_err());
    assert_eq!(version(&conn), 1);
    assert!(conn.is_autocommit());
    conn.execute_batch("ALTER TABLE held_requests RENAME TO requests;")
        .unwrap();
    migrate(&conn).unwrap();
    assert_eq!(version(&conn), SCHEMA_VERSION);
}
