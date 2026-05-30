-- DQ Engine metadata store schema
-- Applied automatically on first docker compose up against database: dq_store
--
-- Evaluation modes:  violation | consistency_delta | consistency_mom
-- Check types:       completeness | validity | consistency
-- Source types:      parquet | mariadb_table | hive_table  (app-validated, not DB-constrained)
-- Statuses:          passed | failed | errored
-- Issue statuses:    new | recurring | resolved
-- No expected_value or comparison_operator — thresholds live in check_sql

CREATE TABLE IF NOT EXISTS config_versions (
    config_version      VARCHAR(64)  NOT NULL,
    config_version_date DATETIME(3)  NOT NULL,
    PRIMARY KEY (config_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_config_version_date ON config_versions (config_version_date);

CREATE TABLE IF NOT EXISTS config_store (
    config_version       VARCHAR(64)   NOT NULL,
    check_id             VARCHAR(128)  NOT NULL,
    table_name           VARCHAR(256)  NOT NULL,
    column_name          VARCHAR(256)  NOT NULL,
    data_type            VARCHAR(64)   NULL,
    check_type           VARCHAR(32)   NOT NULL,
    business_glossary    TEXT          NULL,
    business_rule_filter TEXT          NULL,
    deviation_tolerance  DECIMAL(18,6) NULL,
    check_sql            TEXT          NOT NULL,
    evaluation_mode      VARCHAR(32)   NOT NULL,
    app_code             VARCHAR(64)   NULL,
    PRIMARY KEY (config_version, check_id),
    CONSTRAINT fk_config_store_version
        FOREIGN KEY (config_version) REFERENCES config_versions(config_version),
    CONSTRAINT chk_check_type
        CHECK (check_type IN ('completeness','validity','consistency')),
    CONSTRAINT chk_evaluation_mode
        CHECK (evaluation_mode IN ('violation','consistency_delta','consistency_mom'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_config_store_version ON config_store (config_version);

-- source_type is app-validated (not DB-constrained) to keep the set extensible
CREATE TABLE IF NOT EXISTS source_catalog (
    table_name          VARCHAR(256) NOT NULL,
    source_type         VARCHAR(32)  NOT NULL,
    parquet_location    TEXT         NULL,
    partition_column    VARCHAR(256) NULL,
    connection_ref      VARCHAR(256) NULL,
    physical_table_name VARCHAR(256) NULL,
    filter_column       VARCHAR(256) NULL,
    PRIMARY KEY (table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS result_store (
    dq_ref_id           VARCHAR(64)   NOT NULL,
    check_id            VARCHAR(128)  NOT NULL,
    app_code            VARCHAR(64)   NULL,
    config_version      VARCHAR(64)   NOT NULL,
    config_version_date DATETIME(3)   NOT NULL,
    run_id              VARCHAR(64)   NOT NULL,
    run_timestamp       DATETIME(3)   NOT NULL,
    table_name          VARCHAR(256)  NOT NULL,
    source_type         VARCHAR(32)   NOT NULL,
    column_name         VARCHAR(256)  NOT NULL,
    partition_value     VARCHAR(256)  NULL,
    check_type          VARCHAR(32)   NOT NULL,
    evaluation_mode     VARCHAR(32)   NOT NULL,
    status              VARCHAR(16)   NOT NULL,
    issue_status        VARCHAR(16)   NULL,
    violation_count     BIGINT        NULL,
    current_value       DECIMAL(38,6) NULL,
    prior_value         DECIMAL(38,6) NULL,
    deviation           DECIMAL(38,6) NULL,
    description         TEXT          NULL,
    PRIMARY KEY (dq_ref_id),
    CONSTRAINT fk_result_config_version
        FOREIGN KEY (config_version) REFERENCES config_versions(config_version),
    CONSTRAINT chk_status
        CHECK (status IN ('passed','failed','errored')),
    CONSTRAINT chk_issue_status
        CHECK (issue_status IS NULL OR issue_status IN ('new','recurring','resolved')),
    CONSTRAINT chk_result_check_type
        CHECK (check_type IN ('completeness','validity','consistency')),
    CONSTRAINT chk_result_eval_mode
        CHECK (evaluation_mode IN ('violation','consistency_delta','consistency_mom'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Supports findPreviousResult and findMomResult keyed on (table,col,type,app_code,timestamp)
CREATE INDEX idx_result_key_ts ON result_store (table_name, column_name, check_type, app_code, run_timestamp);
CREATE INDEX idx_result_run_ts ON result_store (run_timestamp);
