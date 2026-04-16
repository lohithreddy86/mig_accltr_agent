-- =============================================================================
-- 03_create_target_tables.sql
-- -----------------------------------------------------------------------------
-- Creates all TARGET tables that Oracle_test_subset.sql writes to, under
-- lz_lakehouse.lm_target_schema (Iceberg on MinIO).
--
-- Scope:
--   Exactly the tables that the migration accelerator's A3 table-registry
--   flags with role=target or role=both for Oracle_test_subset.sql:
--
--     Mis.Lac_Mis_Archive_Status          -> lac_mis_archive_status
--     Mis.Lac_Mis_Archive                 -> lac_mis_archive
--     Mis.Lac_Fu_1                        -> lac_fu_1
--     Mis.Farh_1                          -> farh_1
--     Mis.Farh_2                          -> farh_2
--     Mis.Lac_Paydet_Dpd_Mth              -> lac_paydet_dpd_mth   (49 cols)
--     Mis.Tbl_Eligibility_Intg            -> tbl_eligibility_intg
--     Lacs.Crm_Covid_Customer_Behaviour_Temp -> crm_covid_customer_behaviour_temp
--
--   Source-side tables (oracle_homeloans.*) are intentionally NOT created
--   here — this script only provisions destinations for INSERT / UPDATE /
--   MERGE / TRUNCATE verbs emitted by the converted PySpark code.
--
-- Data-type mapping (Oracle -> Iceberg/Trino):
--     VARCHAR2(n), CHAR(n)  -> VARCHAR
--     NUMBER (id/count)     -> BIGINT
--     NUMBER (amount/rate)  -> DECIMAL(18,4)
--     DATE                  -> TIMESTAMP(6)     (Oracle DATE carries a time)
--     TIMESTAMP(n)          -> TIMESTAMP(6)
--
-- Idempotency:
--     All statements use IF NOT EXISTS. Re-running is safe. To reset a table
--     between test runs, DROP TABLE it explicitly (see REFRESH section at
--     bottom of this file — commented out).
--
-- How to run:
--     docker exec -i trino trino --execute "$(cat infrastructure/init/03_create_target_tables.sql)"
--   OR via the project's MCP helper:
--     .venv/bin/python3 -c "
--     from sql_migration.core.mcp_client import MCPClientSync
--     mcp = MCPClientSync()
--     with open('infrastructure/init/03_create_target_tables.sql') as f:
--       for stmt in [s.strip() for s in f.read().split(';') if s.strip() and not s.strip().startswith('--')]:
--         mcp.run_query(stmt)
--     "
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Schema (idempotent; already created by 01_create_schemas.sql but kept here
-- so this script is self-contained if executed standalone).
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS lz_lakehouse.lm_target_schema
    WITH (location = 's3a://lz-lakehouse/lm_target_schema');


-- =============================================================================
-- 1. lac_mis_archive_status  (audit log)
--    Writes at: lines 38-41, 45-49, 59-63, 73-76, 131-135, 145-149, 159-162,
--               276-280, 290-294, 305-308, 374-378, 388-392, 407-410, 479-483,
--               492-496
--    INSERT cols: (Name, Dwld_Date, Start_Date)
--    UPDATE cols: Status, End_Date, Error
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.lac_mis_archive_status (
    name        VARCHAR    COMMENT 'Job name / run tag (e.g. SAMPLE, POP_LAC_MIS_ARCH3)',
    dwld_date   TIMESTAMP(6) COMMENT 'Download/bucket date — Trunc(Sysdate,''MI'')',
    start_date  TIMESTAMP(6) COMMENT 'Job start timestamp',
    end_date    TIMESTAMP(6) COMMENT 'Job end timestamp — set by UPDATE',
    status      VARCHAR    COMMENT 'NULL while running, ''Y'' success, ''N'' failure',
    error       VARCHAR    COMMENT 'SQLERRM captured on EXCEPTION branch'
);


-- =============================================================================
-- 2. lac_mis_archive  (main archive — UPDATE only, no INSERT in this subset)
--    Writes at: lines 365-372 (Covid_Cust_Category),
--               416-428 (Wo_Flag, Wo_Flag_Dt, Wo_Flag_Upd_By, Wo_Flag_Upd_On),
--               432-436 (Lac_Status),
--               465-475 (Eligibility)
--    Filter cols: Yyyymm, Br_No, Lac_No
--    Read  cols: Customer_Grade, Gr_No  (used in p_Covid_Customer_Category)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.lac_mis_archive (
    lac_no                VARCHAR      COMMENT 'Account number (primary business key)',
    yyyymm                BIGINT       COMMENT 'Reporting month bucket, e.g. 202604',
    br_no                 BIGINT       COMMENT 'Branch number',
    gr_no                 BIGINT       COMMENT 'Group number (used by eligibility logic)',
    customer_grade        VARCHAR      COMMENT 'Upper-cased grade string',
    covid_cust_category   VARCHAR      COMMENT 'Derived covid category tag',
    wo_flag               VARCHAR      COMMENT 'Write-off flag sourced from Lacs.Lac_Master',
    wo_flag_dt            TIMESTAMP(6) COMMENT 'Write-off flag effective date',
    wo_flag_upd_by        VARCHAR      COMMENT 'User who set the WO flag (upstream)',
    wo_flag_upd_on        TIMESTAMP(6) COMMENT 'Timestamp when WO flag was updated',
    lac_status            VARCHAR      COMMENT 'Derived from wo_flag',
    eligibility           VARCHAR      COMMENT 'E / NE derived via tbl_eligibility_intg join'
);


-- =============================================================================
-- 3. lac_fu_1   (follow-up action summary per account)
--    Writes at: line 80 (TRUNCATE), line 82-90 (INSERT SELECT from @Dbloans)
--    INSERT cols from SELECT: Lac_No, Substr(Action,1,1), Max(Date_Of_Action)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.lac_fu_1 (
    lac_no          VARCHAR      COMMENT 'Account number',
    action          VARCHAR      COMMENT 'First char of source Action (V/T/L/G/I/S…)',
    date_of_action  TIMESTAMP(6) COMMENT 'Max date of that action per (lac_no, action)'
);


-- =============================================================================
-- 4. farh_1   (file arm rate history — raw pull)
--    Writes at: line 99 (TRUNCATE), line 102-106 (INSERT SELECT @Dbloans)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.farh_1 (
    lac_no            VARCHAR      COMMENT 'Account number',
    eff_roi_start_dt  TIMESTAMP(6) COMMENT 'Effective rate-of-interest start date',
    revision_id       BIGINT       COMMENT 'Monotonic revision identifier'
);


-- =============================================================================
-- 5. farh_2   (penultimate revision per account — derived from farh_1)
--    Writes at: line 100 (TRUNCATE), line 115-124 (INSERT SELECT Max(...) from farh_1 h)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.farh_2 (
    lac_no                 VARCHAR      COMMENT 'Account number',
    last_roi_revision_dt   TIMESTAMP(6) COMMENT 'Max eff_roi_start_dt from farh_1',
    last_revision_id       BIGINT       COMMENT 'Max revision_id from farh_1'
);


-- =============================================================================
-- 6. lac_paydet_dpd_mth   (49-column monthly payment-detail DPD snapshot)
--    Writes at: line 166 (TRUNCATE), line 167-268 (INSERT SELECT @Dbloans)
--    Column order mirrors the INSERT column list exactly — do NOT reorder.
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.lac_paydet_dpd_mth (
    lac_no                      VARCHAR,
    normal_pay_mode             VARCHAR,
    normal_pay_thro             VARCHAR,
    last_pay_mode               VARCHAR,
    last_pay_thro               VARCHAR,
    paymode_nos                 BIGINT,
    chq_bnc_nos                 BIGINT,
    upd_date                    TIMESTAMP(6),
    row_srno                    BIGINT,
    emi_recd_unposted           DECIMAL(18,4),
    emi_os_unposted             DECIMAL(18,4),
    pmi_recd_unposted           DECIMAL(18,4),
    pmi_os_unposted             DECIMAL(18,4),
    si_recd_unposted            DECIMAL(18,4),
    si_os_unposted              DECIMAL(18,4),
    ai_recd_unposted            DECIMAL(18,4),
    ai_os_unposted              DECIMAL(18,4),
    mr_recd_unposted            DECIMAL(18,4),
    mr_os_unposted              DECIMAL(18,4),
    os_months_emi_unposted      BIGINT,
    os_months_pmi_unposted      BIGINT,
    prin_last_tr_unposted       DECIMAL(18,4),
    ptp_amt                     DECIMAL(18,4),
    ptp_dt                      TIMESTAMP(6),
    ptp_recd                    DECIMAL(18,4),
    unmoved_pdc_amt             DECIMAL(18,4),
    pdc_cnt                     BIGINT,
    das_cnt                     BIGINT,
    bsi_cnt                     BIGINT,
    dcs_cnt                     BIGINT,
    negamrt_revid               BIGINT,
    prin_os_last_tr             DECIMAL(18,4),
    prin_comp_last_tr           DECIMAL(18,4),
    int_comp_last_tr            DECIMAL(18,4),
    last_neg_amrt_revision_id   BIGINT,
    neg_amrt_ind                VARCHAR,
    das_let_prnt_dt             TIMESTAMP(6),
    prin_os_last_tr_comb        DECIMAL(18,4),
    months_os_comb              BIGINT,
    customer_grade              VARCHAR,
    customer_grade2             VARCHAR,
    cure_type                   VARCHAR,
    int_os_last_tr_comb         DECIMAL(18,4),
    prin_comp_os                DECIMAL(18,4),
    pipeline_amt                DECIMAL(18,4),
    pipeline_dt                 TIMESTAMP(6),
    pipeline_proj_bucket        VARCHAR,
    pipeline_code               VARCHAR,
    creation_date               TIMESTAMP(6),
    ai_os_comb                  DECIMAL(18,4)
);


-- =============================================================================
-- 7. tbl_eligibility_intg   (staging table for eligibility join)
--    Writes at: line 440 (TRUNCATE), line 442-457 (INSERT /*+Append*/ SELECT)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.tbl_eligibility_intg (
    lac_no       VARCHAR      COMMENT 'Account number',
    gr_no        BIGINT       COMMENT 'Group number',
    eligibility  VARCHAR      COMMENT 'E = eligible, NE = not eligible',
    dwld_date    TIMESTAMP(6) COMMENT 'Insert timestamp (Sysdate in Oracle)'
);


-- =============================================================================
-- 8. crm_covid_customer_behaviour_temp
--    Writes at: line 313 (TRUNCATE), 314-327 (INSERT SELECT),
--               332-339 (UPDATE customer_grade_curr),
--               341-355 (UPDATE covid_cust_tag_curr),
--               357-362 (UPDATE covid_cust_tag_final, dwld_date)
--    6 cols populated by INSERT; 4 more added by subsequent UPDATEs.
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.crm_covid_customer_behaviour_temp (
    lac_no                 VARCHAR,
    file_no                VARCHAR,
    customer_grade_feb20   VARCHAR,
    covid_cust_tag_feb20   VARCHAR,
    customer_grade_mar21   VARCHAR,
    covid_cust_tag_mar21   VARCHAR,
    customer_grade_curr    VARCHAR      COMMENT 'Populated by UPDATE from Mis.Lac_Mis_Archive',
    covid_cust_tag_curr    VARCHAR      COMMENT 'Populated by UPDATE with categorical rules',
    covid_cust_tag_final   VARCHAR      COMMENT 'COALESCE(curr, mar21, feb20) via UPDATE',
    dwld_date              TIMESTAMP(6)
);


-- -----------------------------------------------------------------------------
-- Verify: list target tables — expect 8 (plus any pre-existing like smoke_test)
-- -----------------------------------------------------------------------------
SHOW TABLES FROM lz_lakehouse.lm_target_schema;


-- -----------------------------------------------------------------------------
-- REFRESH (commented) — uncomment any line to reset that table between test
-- runs. DROP is unavoidable because Trino's Iceberg connector doesn't support
-- TRUNCATE on partition-less tables in every version.
-- -----------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.lac_mis_archive_status;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.lac_mis_archive;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.lac_fu_1;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.farh_1;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.farh_2;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.lac_paydet_dpd_mth;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.tbl_eligibility_intg;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.crm_covid_customer_behaviour_temp;
