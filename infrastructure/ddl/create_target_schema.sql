-- =============================================================================
-- create_target_schema.sql
-- -----------------------------------------------------------------------------
-- Target-catalog DDL for the Oracle_test_subset.sql migration demo.
--
-- Creates one schema and eight business tables that receive every write
-- emitted by the migrated PySpark/Trino code (INSERT / UPDATE / MERGE /
-- TRUNCATE + reload).  Source-side tables are NOT in scope — they live in
-- the read-only source catalog and are not created by this script.
--
-- Tables:
--   1. lac_mis_archive_status               audit log (job name + status)
--   2. lac_mis_archive                      main archive — UPDATE target
--   3. lac_fu_1                             follow-up action summary
--   4. farh_1                               rate-history raw pull
--   5. farh_2                               last-revision summary (from farh_1)
--   6. lac_paydet_dpd_mth                   49-column monthly DPD snapshot
--   7. tbl_eligibility_intg                 eligibility-join staging
--   8. crm_covid_customer_behaviour_temp    covid-tagging staging
--
-- -----------------------------------------------------------------------------
-- BEFORE RUNNING
-- -----------------------------------------------------------------------------
-- 1. Decide the catalog + schema. Defaults below target
--        lz_lakehouse.lm_target_schema
--    If your organisation uses different names, do a single Find & Replace on
--    this file before executing:
--        lz_lakehouse       ->  <your target catalog>
--        lm_target_schema   ->  <your target schema>
--
-- 2. Ensure the target catalog exists and your role has CREATE SCHEMA /
--    CREATE TABLE privileges. If the schema is pre-provisioned by a platform
--    team, comment out the CREATE SCHEMA statement.
--
-- -----------------------------------------------------------------------------
-- HOW TO RUN
-- -----------------------------------------------------------------------------
--   Trino CLI:
--     trino --server <trino-host>:<port> --catalog lz_lakehouse \
--           --schema lm_target_schema --file create_target_schema.sql
--
--   DBeaver / any JDBC tool:
--     Connect to the Trino endpoint, open this file, execute as a script.
--
-- -----------------------------------------------------------------------------
-- TYPE MAPPING (Oracle -> Trino)
-- -----------------------------------------------------------------------------
--     VARCHAR2(n), CHAR(n)   ->  VARCHAR           (unbounded; add length if
--                                                    your engine requires it)
--     NUMBER  (id / count)   ->  BIGINT
--     NUMBER  (amount/rate)  ->  DECIMAL(18,4)     (representative, not tuned)
--     DATE                   ->  TIMESTAMP(6)       (Oracle DATE carries time)
--     TIMESTAMP(n)           ->  TIMESTAMP(6)
--
-- Rerunning is safe — every CREATE uses IF NOT EXISTS. To reset a single
-- table between demo runs, uncomment the matching DROP at the bottom.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Schema
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS lz_lakehouse.lm_target_schema;


-- =============================================================================
-- 1. lac_mis_archive_status     — audit log (INSERT + UPDATE)
--     INSERTed at the start of every proc; UPDATEd on success/failure.
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.lac_mis_archive_status (
    name         VARCHAR       COMMENT 'Job name / run tag (e.g. SAMPLE, POP_LAC_MIS_ARCH3)',
    dwld_date    TIMESTAMP(6)  COMMENT 'Download / bucket date — Trunc(Sysdate,''MI'')',
    start_date   TIMESTAMP(6)  COMMENT 'Job start timestamp',
    end_date     TIMESTAMP(6)  COMMENT 'Job end timestamp — set by UPDATE',
    status       VARCHAR       COMMENT 'NULL while running, ''Y'' success, ''N'' failure',
    error        VARCHAR       COMMENT 'SQLERRM captured on EXCEPTION branch'
);


-- =============================================================================
-- 2. lac_mis_archive            — main archive (UPDATE only in this subset)
--     Four distinct UPDATE shapes (covid tag, write-off flags, lac status,
--     eligibility). WHERE clauses filter by (yyyymm, br_no, lac_no).
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.lac_mis_archive (
    lac_no                VARCHAR       COMMENT 'Account number (primary business key)',
    yyyymm                BIGINT        COMMENT 'Reporting month bucket, e.g. 202604',
    br_no                 BIGINT        COMMENT 'Branch number',
    gr_no                 BIGINT        COMMENT 'Group number (used by eligibility logic)',
    customer_grade        VARCHAR       COMMENT 'Upper-cased grade string',
    covid_cust_category   VARCHAR       COMMENT 'Derived covid category tag',
    wo_flag               VARCHAR       COMMENT 'Write-off flag sourced from Lac_Master',
    wo_flag_dt            TIMESTAMP(6)  COMMENT 'Write-off flag effective date',
    wo_flag_upd_by        VARCHAR       COMMENT 'User who set the WO flag (upstream)',
    wo_flag_upd_on        TIMESTAMP(6)  COMMENT 'Timestamp when WO flag was updated',
    lac_status            VARCHAR       COMMENT 'Derived from wo_flag',
    eligibility           VARCHAR       COMMENT 'E / NE derived via tbl_eligibility_intg join'
);


-- =============================================================================
-- 3. lac_fu_1                   — follow-up action summary
--     TRUNCATE + INSERT: (lac_no, first-char of action, MAX(action_date)).
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.lac_fu_1 (
    lac_no           VARCHAR       COMMENT 'Account number',
    action           VARCHAR       COMMENT 'First char of source Action (V/T/L/G/I/S…)',
    date_of_action   TIMESTAMP(6)  COMMENT 'MAX(date_of_action) per (lac_no, action)'
);


-- =============================================================================
-- 4. farh_1                     — file arm rate history (raw pull)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.farh_1 (
    lac_no             VARCHAR       COMMENT 'Account number',
    eff_roi_start_dt   TIMESTAMP(6)  COMMENT 'Effective rate-of-interest start date',
    revision_id        BIGINT        COMMENT 'Monotonic revision identifier'
);


-- =============================================================================
-- 5. farh_2                     — last-revision summary (derived from farh_1)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.farh_2 (
    lac_no                  VARCHAR       COMMENT 'Account number',
    last_roi_revision_dt    TIMESTAMP(6)  COMMENT 'MAX(eff_roi_start_dt) from farh_1',
    last_revision_id        BIGINT        COMMENT 'MAX(revision_id)      from farh_1'
);


-- =============================================================================
-- 6. lac_paydet_dpd_mth         — 49-column monthly payment-detail DPD snapshot
--     TRUNCATE + INSERT from the remote Lac_Paydet_Dpd source. Column order
--     mirrors the source INSERT list exactly — do NOT reorder.
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.lac_paydet_dpd_mth (
    lac_no                       VARCHAR,
    normal_pay_mode              VARCHAR,
    normal_pay_thro              VARCHAR,
    last_pay_mode                VARCHAR,
    last_pay_thro                VARCHAR,
    paymode_nos                  BIGINT,
    chq_bnc_nos                  BIGINT,
    upd_date                     TIMESTAMP(6),
    row_srno                     BIGINT,
    emi_recd_unposted            DECIMAL(18,4),
    emi_os_unposted              DECIMAL(18,4),
    pmi_recd_unposted            DECIMAL(18,4),
    pmi_os_unposted              DECIMAL(18,4),
    si_recd_unposted             DECIMAL(18,4),
    si_os_unposted               DECIMAL(18,4),
    ai_recd_unposted             DECIMAL(18,4),
    ai_os_unposted               DECIMAL(18,4),
    mr_recd_unposted             DECIMAL(18,4),
    mr_os_unposted               DECIMAL(18,4),
    os_months_emi_unposted       BIGINT,
    os_months_pmi_unposted       BIGINT,
    prin_last_tr_unposted        DECIMAL(18,4),
    ptp_amt                      DECIMAL(18,4),
    ptp_dt                       TIMESTAMP(6),
    ptp_recd                     DECIMAL(18,4),
    unmoved_pdc_amt              DECIMAL(18,4),
    pdc_cnt                      BIGINT,
    das_cnt                      BIGINT,
    bsi_cnt                      BIGINT,
    dcs_cnt                      BIGINT,
    negamrt_revid                BIGINT,
    prin_os_last_tr              DECIMAL(18,4),
    prin_comp_last_tr            DECIMAL(18,4),
    int_comp_last_tr             DECIMAL(18,4),
    last_neg_amrt_revision_id    BIGINT,
    neg_amrt_ind                 VARCHAR,
    das_let_prnt_dt              TIMESTAMP(6),
    prin_os_last_tr_comb         DECIMAL(18,4),
    months_os_comb               BIGINT,
    customer_grade               VARCHAR,
    customer_grade2              VARCHAR,
    cure_type                    VARCHAR,
    int_os_last_tr_comb          DECIMAL(18,4),
    prin_comp_os                 DECIMAL(18,4),
    pipeline_amt                 DECIMAL(18,4),
    pipeline_dt                  TIMESTAMP(6),
    pipeline_proj_bucket         VARCHAR,
    pipeline_code                VARCHAR,
    creation_date                TIMESTAMP(6),
    ai_os_comb                   DECIMAL(18,4)
);


-- =============================================================================
-- 7. tbl_eligibility_intg       — eligibility-join staging
--     TRUNCATE + INSERT /*+APPEND*/ per run; joined back into lac_mis_archive.
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.tbl_eligibility_intg (
    lac_no        VARCHAR       COMMENT 'Account number',
    gr_no         BIGINT        COMMENT 'Group number',
    eligibility   VARCHAR       COMMENT 'E = eligible, NE = not eligible',
    dwld_date     TIMESTAMP(6)  COMMENT 'Insert timestamp (Sysdate in Oracle)'
);


-- =============================================================================
-- 8. crm_covid_customer_behaviour_temp  — covid-tagging staging
--     TRUNCATE + INSERT populates the first 6 columns; three later UPDATEs
--     fill customer_grade_curr, covid_cust_tag_curr, covid_cust_tag_final,
--     and dwld_date.
-- =============================================================================
CREATE TABLE IF NOT EXISTS lz_lakehouse.lm_target_schema.crm_covid_customer_behaviour_temp (
    lac_no                   VARCHAR,
    file_no                  VARCHAR,
    customer_grade_feb20     VARCHAR,
    covid_cust_tag_feb20     VARCHAR,
    customer_grade_mar21     VARCHAR,
    covid_cust_tag_mar21     VARCHAR,
    customer_grade_curr      VARCHAR       COMMENT 'Populated by UPDATE from lac_mis_archive',
    covid_cust_tag_curr      VARCHAR       COMMENT 'Populated by UPDATE with categorical rules',
    covid_cust_tag_final     VARCHAR       COMMENT 'COALESCE(curr, mar21, feb20) via UPDATE',
    dwld_date                TIMESTAMP(6)
);


-- -----------------------------------------------------------------------------
-- Verify — expect all 8 tables listed
-- -----------------------------------------------------------------------------
SHOW TABLES FROM lz_lakehouse.lm_target_schema;


-- -----------------------------------------------------------------------------
-- RESET HELPERS (commented) — uncomment a line to drop + recreate that table
-- between demo runs. Some engines don't support TRUNCATE on unpartitioned
-- lakehouse tables, so DROP + CREATE is the reliable reset.
-- -----------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.lac_mis_archive_status;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.lac_mis_archive;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.lac_fu_1;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.farh_1;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.farh_2;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.lac_paydet_dpd_mth;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.tbl_eligibility_intg;
-- DROP TABLE IF EXISTS lz_lakehouse.lm_target_schema.crm_covid_customer_behaviour_temp;
