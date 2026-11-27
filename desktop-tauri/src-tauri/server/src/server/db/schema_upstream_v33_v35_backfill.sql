-- 补偿迁移 v38 专用的幂等建表脚本。
--
-- 内容 = 上游 v33 (schema_task_plugins.sql) 全文 + 上游 v35
-- (schema_local_direct.sql) 去掉首行 `ALTER TABLE requests ADD COLUMN origin`
-- 之后的部分。所有语句都是 CREATE TABLE / CREATE INDEX IF NOT EXISTS，
-- 重复执行是空操作。
--
-- 为什么单独放一份而不是直接 include_str 上游那两个文件：
--   · schema_local_direct.sql 首行是裸 ALTER（非幂等），整段重放会报
--     "duplicate column name"；
--   · schema_privacy_redaction.sql 整条都是裸 ALTER，同样不能整段重放。
-- 两条 ALTER 由 Rust 侧的 ensure_upstream_v33_v35 用 PRAGMA table_info
-- 逐列守门后补。
--
-- 这是一份**历史快照**：上游将来若改这两组表的结构，本文件不要跟着改 ——
-- 它的职责只是把从 fork 旧谱系跳过 v33/v34 的库补齐到 3.6.0 当时的形态。
-- 新库的真实定义仍以 v33/v35 的原始 SQL 为准。

-- ── 源自 schema_task_plugins.sql（v33）─────────────────────────
CREATE TABLE IF NOT EXISTS task_plugins (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  version TEXT NOT NULL DEFAULT '0.1.0',
  source TEXT NOT NULL DEFAULT 'local',
  signed INTEGER NOT NULL DEFAULT 0,
  enabled INTEGER NOT NULL DEFAULT 1,
  icon TEXT NOT NULL DEFAULT '',
  default_cron TEXT NOT NULL DEFAULT '0 9 * * *',
  default_timezone TEXT NOT NULL DEFAULT 'Asia/Shanghai',
  default_timeout INTEGER NOT NULL DEFAULT 30,
  domains_json TEXT NOT NULL DEFAULT '[]',
  declares_notify INTEGER NOT NULL DEFAULT 0,
  notify_allowed INTEGER NOT NULL DEFAULT 1,
  fields_json TEXT NOT NULL DEFAULT '[]',
  actions_json TEXT NOT NULL DEFAULT '[]',
  status_fields_json TEXT NOT NULL DEFAULT '[]',
  script_body TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS task_jobs (
  id TEXT PRIMARY KEY,
  plugin_id TEXT NOT NULL,
  name TEXT NOT NULL,
  cron_expression TEXT NOT NULL,
  timezone TEXT NOT NULL DEFAULT 'Asia/Shanghai',
  timeout_seconds INTEGER NOT NULL DEFAULT 30,
  proxy_id TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 1,
  values_json TEXT NOT NULL DEFAULT '{}',
  secrets_enc TEXT NOT NULL DEFAULT '',
  storage_enc TEXT NOT NULL DEFAULT '',
  status_summary_json TEXT NOT NULL DEFAULT '{}',
  next_run_at INTEGER,
  last_run_at INTEGER,
  last_status TEXT,
  last_duration_ms INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_jobs_due
  ON task_jobs(enabled, next_run_at);

CREATE INDEX IF NOT EXISTS idx_task_jobs_plugin
  ON task_jobs(plugin_id);

CREATE TABLE IF NOT EXISTS task_job_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  trigger_type TEXT NOT NULL,
  trigger_label TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  finished_at INTEGER,
  duration_ms INTEGER,
  output TEXT,
  error TEXT,
  FOREIGN KEY (job_id) REFERENCES task_jobs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_task_job_runs_job
  ON task_job_runs(job_id, id DESC);

-- ── 源自 schema_local_direct.sql（v35，去掉首行 ALTER origin）──
CREATE INDEX IF NOT EXISTS idx_requests_origin_ts ON requests(origin, ts DESC, row_id DESC);

CREATE TABLE IF NOT EXISTS local_direct_refs (
    request_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    root_kind TEXT NOT NULL DEFAULT '',
    source_key TEXT NOT NULL UNIQUE,
    conversation_id TEXT NOT NULL DEFAULT '',
    conversation_title TEXT NOT NULL DEFAULT '',
    workspace_uri TEXT NOT NULL DEFAULT '',
    db_path TEXT NOT NULL DEFAULT '',
    transcript_path TEXT NOT NULL DEFAULT '',
    step_index INTEGER NOT NULL DEFAULT 0,
    response_id TEXT NOT NULL DEFAULT '',
    ts INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_local_direct_refs_agent_ts ON local_direct_refs(agent_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_local_direct_refs_conv ON local_direct_refs(conversation_id);

CREATE TABLE IF NOT EXISTS local_direct_files (
    file_path TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    mtime_ms INTEGER NOT NULL DEFAULT 0,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    last_step_idx INTEGER NOT NULL DEFAULT -1,
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_local_direct_files_agent ON local_direct_files(agent_id);
