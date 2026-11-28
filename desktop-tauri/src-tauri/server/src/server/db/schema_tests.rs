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
/// 35 照上游跑，36/37 再跑 fork 旧谱系的 logs 索引 + 补列 —— 这样
/// user_version=37 且恰好缺 v33/v34 的对象。
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
    apply_version(&conn, 35, &mut cleaned).unwrap();
    // fork 旧谱系的 v36/v37：logs 索引 + 补列，而不是上游的本机直连归正 /
    // 请求协议列。
    conn.execute_batch(include_str!("schema_logs_level.sql"))
        .unwrap();
    ensure_columns_34(&conn).unwrap();
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
    ensure_upstream_v36_v38(&conn).unwrap();
    assert_eq!(table_names(&conn), tables);
    assert!(has_column(&conn, "requests", "client_protocol"));
    assert!(has_column(&conn, "requests", "upstream_protocol"));
    migrate(&conn).unwrap();
    assert_eq!(version(&conn), SCHEMA_VERSION);
}

/// 从 fork 3.6.1-beta-1（user_version=38，v36-v38 分别是 logs 索引 / 补列 /
/// task 补偿）升上来的库，必须补回上游 3.6.1 的本机直连归正、两条请求协议列
/// 和 `api_key_usage_events` 表。缺了后两者，请求详情按新协议读列会报
/// `no such column`，API Key 用量统计整块失效。
#[test]
fn fork_v38_database_gains_the_upstream_v36_v38_objects() {
    let conn = Connection::open_in_memory().unwrap();
    let mut cleaned = 0;
    for version in 1..=32 {
        apply_version(&conn, version, &mut cleaned).unwrap();
    }
    // fork 旧谱系 v33/v34。
    conn.execute_batch(include_str!("schema_logs_level.sql"))
        .unwrap();
    ensure_columns_34(&conn).unwrap();
    // v35 上游本机直连表。
    apply_version(&conn, 35, &mut cleaned).unwrap();
    // fork 旧谱系 v36/v37。
    conn.execute_batch(include_str!("schema_logs_level.sql"))
        .unwrap();
    ensure_columns_34(&conn).unwrap();
    // fork 旧谱系 v38：task 三表 / privacy_hits 补偿。
    ensure_upstream_v33_v35(&conn).unwrap();
    conn.pragma_update(None, "user_version", 38).unwrap();

    assert!(!has_column(&conn, "requests", "client_protocol"), "前置条件：fork 旧库不应有 client_protocol 列");
    assert!(!has_column(&conn, "requests", "upstream_protocol"), "前置条件：fork 旧库不应有 upstream_protocol 列");
    assert!(
        !table_names(&conn).iter().any(|t| t == "api_key_usage_events"),
        "前置条件：fork 旧库不应有 api_key_usage_events 表"
    );

    migrate(&conn).unwrap();

    assert_eq!(version(&conn), SCHEMA_VERSION);
    assert!(has_column(&conn, "requests", "client_protocol"), "升级后必须有 client_protocol 列");
    assert!(has_column(&conn, "requests", "upstream_protocol"), "升级后必须有 upstream_protocol 列");
    assert!(
        table_names(&conn).iter().any(|t| t == "api_key_usage_events"),
        "升级后必须有 api_key_usage_events 表"
    );
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

#[test]
fn v36_migrates_local_direct_antigravity_requests_and_daily_provider_stats() {
    let conn = Connection::open_in_memory().unwrap();
    let mut cleaned = 0;
    for v in 1..=35 {
        apply_version(&conn, v, &mut cleaned).unwrap();
    }
    conn.pragma_update(None, "user_version", 35).unwrap();

    conn.execute(
        "INSERT INTO requests (id, ts, model, account_id, account_name, status, provider, client, origin) \
         VALUES ('req-ld-1', 1760000000000, 'gemini-4-neon', 'local-direct', '本机直连', 200, 'local-direct', 'Antigravity', 'local_direct')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO local_direct_refs (request_id, agent_id, root_kind, source_key, ts) \
         VALUES ('req-ld-1', 'antigravity', 'desktop', 'k-1', 1760000000000)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO requests (id, ts, model, account_id, account_name, status, provider, client, origin) \
         VALUES ('req-ld-2', 1760000001000, 'gemini-4-neon', 'local-direct:antigravity:custom@example.com', '我的号', 200, 'local-direct', 'Antigravity', 'local_direct')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO local_direct_refs (request_id, agent_id, root_kind, source_key, ts) \
         VALUES ('req-ld-2', 'antigravity', 'cli', 'k-2', 1760000001000)",
        [],
    )
    .unwrap();

    let daily_stats = serde_json::json!([
        {"provider": "antigravity", "requests": 2, "successful": 2, "tokens": 100, "costUsd": 0.1, "unpricedRequests": 0},
        {"provider": "local-direct", "requests": 3, "successful": 3, "tokens": 250, "costUsd": 0.25, "unpricedRequests": 1}
    ])
    .to_string();
    conn.execute(
        "INSERT INTO request_daily (date, requests, successful, tokens, provider_stats) \
         VALUES ('2026-11-26', 5, 5, 350, ?1)",
        [daily_stats],
    )
    .unwrap();

    migrate(&conn).unwrap();
    assert_eq!(version(&conn), SCHEMA_VERSION);

    let (p1, aid1, aname1): (String, String, String) = conn
        .query_row(
            "SELECT provider, account_id, account_name FROM requests WHERE id='req-ld-1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(p1, "antigravity");
    assert_eq!(aid1, "local-direct:antigravity:desktop");
    assert_eq!(aname1, "桌面版");

    let (p2, aid2, aname2): (String, String, String) = conn
        .query_row(
            "SELECT provider, account_id, account_name FROM requests WHERE id='req-ld-2'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(p2, "antigravity");
    assert_eq!(aid2, "local-direct:antigravity:custom@example.com");
    assert_eq!(aname2, "我的号");

    let merged_raw: String = conn
        .query_row(
            "SELECT provider_stats FROM request_daily WHERE date='2026-11-26'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let merged: Vec<serde_json::Value> = serde_json::from_str(&merged_raw).unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0]["provider"], "antigravity");
    assert_eq!(merged[0]["requests"], 5);
    assert_eq!(merged[0]["successful"], 5);
    assert_eq!(merged[0]["tokens"], 350);
    assert_eq!(merged[0]["unpricedRequests"], 1);
}
