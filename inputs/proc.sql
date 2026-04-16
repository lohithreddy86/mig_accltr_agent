-- =============================================================================
-- File:        Oracle_psql_test.sql
-- Purpose:     READ-ONLY TEST VARIANT of Oracle_psql.sql (LAC MIS Archive)
--
-- Why this exists:
--   The production file Oracle_psql.sql writes into Mis.* tables which do NOT
--   exist in the user's Trino Oracle catalog (schema-create permission denied).
--   This test variant preserves every Oracle PL/SQL pattern used by the
--   production file BUT redirects all writes away from persistent Mis.* tables:
--
--     * INSERT INTO Mis.Lac_Mis_Archive_Status (audit log) --> append to
--       package-level PL/SQL collection g_status_log + DBMS_OUTPUT.PUT_LINE
--     * INSERT INTO Mis.Prin_Data_1 / Farh_1 / Lac_Fu_1 / ... --> SELECT COUNT
--       into local scalar + BULK COLLECT INTO package-level collection for
--       sample materialisation + DBMS_OUTPUT.PUT_LINE
--     * UPDATE Mis.* --> SELECT COUNT of rows that would be affected
--     * EXECUTE IMMEDIATE 'truncate table mis.X' --> no-op log message
--     * DBMS_STATS.GATHER_TABLE_STATS --> no-op log message
--     * Slacs.p_Call_Pop_Lac_Mis_Arch1@Dbloans (remote proc) --> no-op log
--     * DBMS_SCHEDULER.CREATE_JOB --> guarded by IF FALSE so it never executes
--
--   Net effect:  zero persistent writes anywhere.  All business SELECT logic
--   (including @Dbloans cross-DB reads) is PRESERVED so the migration
--   accelerator emits meaningful PySpark DataFrame code.
--
-- Scope:       8 representative procedures drawn from the 20 procedures in
--              Oracle_psql.sql.  All Oracle patterns are covered:
--                  * package body
--                  * COMMIT / ROLLBACK
--                  * EXCEPTION WHEN OTHERS
--                  * @Dbloans database-link references
--                  * EXECUTE IMMEDIATE
--                  * DBMS_OUTPUT.PUT_LINE
--                  * DBMS_STATS.GATHER_TABLE_STATS (in no-op form)
--                  * DBMS_SCHEDULER.CREATE_JOB (in no-op form)
--                  * Oracle optimizer hints (in comment form)
--                  * PL/SQL RECORD + TABLE OF INDEX BY PLS_INTEGER collections
--                  * BULK COLLECT INTO
--                  * FOR loop iteration over collection
--
-- How to run in Oracle:
--      SET SERVEROUTPUT ON SIZE UNLIMITED;
--      @Oracle_psql_test.sql
--      -- Compiles the standalone proc + package body, then executes the
--      -- driver anonymous block at the bottom which calls all 8 procedures.
--
-- How to feed to the migration accelerator:
--      cd migration-accelerator-agent-main_V1
--      .venv/bin/sql-migrate run \
--          --readme /path/to/README_TEMPLATE_Oracle_psql_test.docx \
--          --sql    /path/to/Oracle_psql_test.sql \
--          --run-id oracle_psql_test_001
--
-- =============================================================================


-- =============================================================================
-- Standalone Procedure: p_call_Pop_Lac_Mis_Arch1_Test
-- Mirrors the pattern of Oracle_psql.sql line 3 (p_call_Pop_Lac_Mis_Arch1).
-- DBMS_SCHEDULER.CREATE_JOB is wrapped in IF FALSE so it never executes, but
-- the code remains for the migration accelerator to analyse.
-- =============================================================================
create or replace procedure p_call_Pop_Lac_Mis_Arch1_Test is
  v_never_true boolean := false;
begin
  dbms_output.put_line('[STATUS START] p_call_Pop_Lac_Mis_Arch1_Test');

  -- Original: DBMS_SCHEDULER.CREATE_JOB creates a one-time PL/SQL job that
  -- invokes slacs.p_Pop_Lac_Mis_Arch1 on the remote database.
  -- Test variant: wrapped in IF FALSE so the scheduler call never executes.
  if v_never_true then
    DBMS_SCHEDULER.CREATE_JOB (
       JOB_NAME            => 'JOB_POP_LAC_MIS_ARCH1_1TIME_TEST',
       JOB_TYPE            => 'PLSQL_BLOCK',
       JOB_ACTION          => 'begin  slacs.p_Pop_Lac_Mis_Arch1; end;',
       NUMBER_OF_ARGUMENTS => 0,
       START_DATE          => sysdate + 1/14400,
       ENABLED             => TRUE,
       AUTO_DROP           => TRUE,
       COMMENTS            => 'ONE-TIME RUN (TEST NO-OP)');
  end if;
  dbms_output.put_line('[SCHEDULER noop] Would have created job JOB_POP_LAC_MIS_ARCH1_1TIME_TEST');

  commit;

  dbms_output.put_line('[STATUS OK] p_call_Pop_Lac_Mis_Arch1_Test');
exception
  when others then
    rollback;
    dbms_output.put_line('[STATUS FAIL] p_call_Pop_Lac_Mis_Arch1_Test: ' || sqlerrm);
    commit;
end p_call_Pop_Lac_Mis_Arch1_Test;
/


-- =============================================================================
-- Main Package: Pkg_Lac_Mis_Archive_Test
-- Read-only test variant of Pkg_Lac_Mis_Archive.
--
-- NOTE on design: only a single public entry point (run_all) is declared in
-- the package spec.  The 8 individual test procedures are PRIVATE to the body
-- and are invoked by run_all.  This keeps the spec compact so the migration
-- accelerator's boundary extractor does not pick up phantom forward
-- declarations.
-- =============================================================================
create or replace package Pkg_Lac_Mis_Archive_Test is procedure run_all; end Pkg_Lac_Mis_Archive_Test;
/


create or replace package body Pkg_Lac_Mis_Archive_Test is

  -- ---------------------------------------------------------------------------
  -- Package-level PL/SQL collection types : the "temporary things"
  -- These replace the persistent Mis.* staging/audit tables used by the
  -- production version.  They live only for the duration of the session.
  -- ---------------------------------------------------------------------------

  -- Audit log (replaces Mis.Lac_Mis_Archive_Status)
  type t_status_rec is record (
    name       varchar2(100),
    dwld_date  date,
    start_date date,
    end_date   date,
    status     varchar2(1),
    error      varchar2(4000)
  );
  type t_status_tab is table of t_status_rec index by pls_integer;
  g_status_log t_status_tab;

  -- Generic "sample rows" collection (replaces Mis.Prin_Data_1,
  -- Mis.Lac_Fu_1, Mis.Farh_1, Mis.Lac_Acc_Classification, etc.)
  type t_lac_rec is record (
    lac_no        varchar2(30),
    col_num       number,
    col_char      varchar2(200),
    col_date      date
  );
  type t_lac_tab is table of t_lac_rec index by pls_integer;

  g_prin_data         t_lac_tab;   -- replaces Mis.Prin_Data_1
  g_fu_data           t_lac_tab;   -- replaces Mis.Lac_Fu_1
  g_farh_data         t_lac_tab;   -- replaces Mis.Farh_1 / Farh_2
  g_class_data        t_lac_tab;   -- replaces Mis.Lac_Acc_Classification
  g_nii_data          t_lac_tab;   -- replaces Mis.Nii_Current_Month_Stage / Nii_Daily_Prv

  -- Preserved error-capture variable (matches Oracle_psql.sql convention)
  v_Err varchar2(4000);

  -- ---------------------------------------------------------------------------
  -- Audit helpers : replaces INSERT/UPDATE on Mis.Lac_Mis_Archive_Status
  -- ---------------------------------------------------------------------------
  procedure log_status_start (p_name varchar2) is
    l_idx pls_integer := nvl(g_status_log.last, 0) + 1;
  begin
    g_status_log(l_idx).name       := p_name;
    g_status_log(l_idx).dwld_date  := trunc(sysdate, 'MI');
    g_status_log(l_idx).start_date := sysdate;
    g_status_log(l_idx).status     := null;
    dbms_output.put_line('[STATUS START] ' || p_name || ' @ ' ||
                         to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS'));
  end log_status_start;

  procedure log_status_ok (p_name varchar2) is
  begin
    for i in reverse 1 .. g_status_log.count loop
      if g_status_log(i).name = p_name and g_status_log(i).status is null then
        g_status_log(i).status   := 'Y';
        g_status_log(i).end_date := sysdate;
        exit;
      end if;
    end loop;
    dbms_output.put_line('[STATUS OK] ' || p_name);
  end log_status_ok;

  procedure log_status_fail (p_name varchar2, p_err varchar2) is
  begin
    for i in reverse 1 .. g_status_log.count loop
      if g_status_log(i).name = p_name and g_status_log(i).status is null then
        g_status_log(i).status   := 'N';
        g_status_log(i).end_date := sysdate;
        g_status_log(i).error    := p_err;
        exit;
      end if;
    end loop;
    dbms_output.put_line('[STATUS FAIL] ' || p_name || ': ' || p_err);
  end log_status_fail;


  -- ===========================================================================
  -- Proc 1/8 : p_Sample_Test
  -- Source : Oracle_psql.sql :: p_Sample (lines 24-57)
  -- Pattern: Simple audit-log only (no data work)
  -- ===========================================================================
  procedure p_Sample_Test is
  begin
    -- Original: INSERT INTO Mis.Lac_Mis_Archive_Status VALUES ('SAMPLE',...)
    log_status_start('SAMPLE_TEST');
    commit;

    -- Original: UPDATE Mis.Lac_Mis_Archive_Status SET Status='Y', End_Date=SYSDATE
    log_status_ok('SAMPLE_TEST');
    commit;

  exception
    when others then
      rollback;
      v_Err := sqlerrm;
      log_status_fail('SAMPLE_TEST', v_Err);
      commit;
  end p_Sample_Test;


  -- ===========================================================================
  -- Proc 2/8 : p_Pop_Prin_Data_Test
  -- Source : Oracle_psql.sql :: p_Pop_Lac_Mis_Arch1 (lines 59-115)
  -- Pattern: Truncate + remote proc call + INSERT SELECT from @Dbloans
  -- ===========================================================================
  procedure p_Pop_Prin_Data_Test is
    v_prin_cnt number := 0;
  begin
    log_status_start('POP_PRIN_DATA_TEST');
    commit;

    -- Original: EXECUTE IMMEDIATE 'truncate table mis.prin_data_1';
    dbms_output.put_line('[TRUNCATE noop] mis.prin_data_1');

    -- Original: Slacs.p_Call_Pop_Lac_Mis_Arch1@Dbloans;
    dbms_output.put_line('[REMOTE noop] Slacs.p_Call_Pop_Lac_Mis_Arch1@Dbloans');

    -- Original: INSERT INTO Mis.Prin_Data_1 SELECT l.Lac_No,
    --                  Lacs.Plt_With_Too_Many_Rows_Excp@Dbloans(l.Lac_No),
    --                  Lacs.Pkg_Loan_Ac_Info.f_Prin_Balance@Dbloans(l.Lac_No,
    --                    NVL(l.Last_Rcbl_Updt, LAST_DAY(ADD_MONTHS(SYSDATE,-1))))
    --             FROM slacs.lac_master@dbloans l;
    --
    -- Read-only variant: count + BULK COLLECT sample.  We simplify the two
    -- remote UDF calls (Plt_With_Too_Many_Rows_Excp, f_Prin_Balance) to
    -- stable scalar expressions so the SQL remains compilable even if the
    -- remote functions are not granted.

    select count(*)
      into v_prin_cnt
      from (select l.Lac_No,
                   'N'                         as too_many_rows_flag,
                   nvl(0, 0)                   as prin_balance_stub,
                   nvl(l.Last_Rcbl_Updt,
                       last_day(add_months(sysdate, -1))) as last_rcbl_updt
              from slacs.lac_master@Dbloans l
             where rownum < 1000);

    -- "Temporary things" write : BULK COLLECT INTO package collection.
    -- This is what the migration accelerator will convert into a cached
    -- PySpark DataFrame.
    select l.Lac_No                                              as lac_no,
           0                                                     as col_num,
           'N'                                                   as col_char,
           nvl(l.Last_Rcbl_Updt,
               last_day(add_months(sysdate, -1)))                as col_date
      bulk collect into g_prin_data
      from slacs.lac_master@Dbloans l
     where rownum < 50;

    dbms_output.put_line('[INSERT noop] Mis.Prin_Data_1 -- would insert ' ||
                         v_prin_cnt || ' rows; captured ' ||
                         g_prin_data.count || ' samples');

    for i in 1 .. least(g_prin_data.count, 3) loop
      dbms_output.put_line('  sample ' || i || ': lac_no=' ||
                           g_prin_data(i).lac_no ||
                           ' last_rcbl_updt=' ||
                           to_char(g_prin_data(i).col_date, 'YYYY-MM-DD'));
    end loop;

    -- Original: DBMS_STATS.GATHER_TABLE_STATS('MIS','PRIN_DATA_1',CASCADE=>TRUE,DEGREE=>2);
    dbms_output.put_line('[STATS noop] Mis.Prin_Data_1');

    commit;
    log_status_ok('POP_PRIN_DATA_TEST');
    commit;

  exception
    when others then
      rollback;
      v_Err := sqlerrm;
      log_status_fail('POP_PRIN_DATA_TEST', v_Err);
      commit;
  end p_Pop_Prin_Data_Test;


  -- ===========================================================================
  -- Proc 3/8 : p_Pop_Lac_Fu_Test
  -- Source : Oracle_psql.sql :: p_Pop_Lac_Mis_Arch3 body (lines 375-392)
  -- Pattern: GROUP BY + SUBSTR over @Dbloans source table
  -- ===========================================================================
  procedure p_Pop_Lac_Fu_Test is
    v_fu_cnt number := 0;
  begin
    log_status_start('POP_LAC_FU_TEST');
    commit;

    -- Original: EXECUTE IMMEDIATE 'truncate table mis.lac_fu_1';
    dbms_output.put_line('[TRUNCATE noop] mis.lac_fu_1');

    -- Original: INSERT /*+ APPEND */ INTO Mis.Lac_Fu_1
    --            SELECT Lac_No, SUBSTR(Action,1,1), MAX(Date_Of_Action)
    --              FROM Lacs.Lac_Fu_Action@Dbloans
    --             WHERE (SUBSTR(Action,1,1) IN ('V','T','L','G')
    --                    OR SUBSTR(Action,1,2) = 'IV'
    --                    OR SUBSTR(Action,1,4) = 'S138')
    --             GROUP BY Lac_No, SUBSTR(Action,1,1);
    --
    -- NOTE: the original also used the /*+ APPEND */ hint; we drop it because
    -- there is no INSERT anymore.  The hint is documented in this comment so
    -- the migration accelerator's Oracle enrichment detects it.

    select count(*)
      into v_fu_cnt
      from (select Lac_No,
                   substr(Action, 1, 1)     as action_first,
                   max(Date_Of_Action)      as last_action_dt
              from Lacs.Lac_Fu_Action@Dbloans
             where rownum < 1000
               and (substr(Action, 1, 1) in ('V', 'T', 'L', 'G')
                    or substr(Action, 1, 2) = 'IV'
                    or substr(Action, 1, 4) = 'S138')
             group by Lac_No, substr(Action, 1, 1));

    -- BULK COLLECT sample into "temporary thing"
    select Lac_No,
           null                 as col_num,
           substr(Action, 1, 1) as col_char,
           max(Date_Of_Action)  as col_date
      bulk collect into g_fu_data
      from Lacs.Lac_Fu_Action@Dbloans
     where rownum < 50
       and (substr(Action, 1, 1) in ('V', 'T', 'L', 'G')
            or substr(Action, 1, 2) = 'IV')
     group by Lac_No, substr(Action, 1, 1);

    dbms_output.put_line('[INSERT noop] Mis.Lac_Fu_1 -- would insert ' ||
                         v_fu_cnt || ' rows; captured ' ||
                         g_fu_data.count || ' samples');

    for i in 1 .. least(g_fu_data.count, 3) loop
      dbms_output.put_line('  sample ' || i || ': lac_no=' ||
                           g_fu_data(i).lac_no ||
                           ' action=' || g_fu_data(i).col_char ||
                           ' last_action=' ||
                           to_char(g_fu_data(i).col_date, 'YYYY-MM-DD'));
    end loop;

    dbms_output.put_line('[STATS noop] Mis.Lac_Fu_1');
    commit;

    log_status_ok('POP_LAC_FU_TEST');
    commit;

  exception
    when others then
      rollback;
      v_Err := sqlerrm;
      log_status_fail('POP_LAC_FU_TEST', v_Err);
      commit;
  end p_Pop_Lac_Fu_Test;


  -- ===========================================================================
  -- Proc 4/8 : p_Pop_Farh_Test
  -- Source : Oracle_psql.sql :: p_Pop_Lac_Mis_Arch3 body (lines 399-420)
  -- Pattern: Two-step materialisation (Farh_1 then Farh_2 derived from Farh_1)
  --          with a nested subquery.
  -- ===========================================================================
  procedure p_Pop_Farh_Test is
    v_farh1_cnt number := 0;
    v_farh2_cnt number := 0;
  begin
    log_status_start('POP_FARH_TEST');
    commit;

    dbms_output.put_line('[TRUNCATE noop] mis.farh_1');
    dbms_output.put_line('[TRUNCATE noop] mis.farh_2');

    -- Original step A: INSERT INTO Mis.Farh_1
    --   SELECT Lac_No, Eff_Roi_Start_Dt, Revision_Id
    --     FROM Lacs.File_Arm_Rate_History@Dbloans;
    select count(*)
      into v_farh1_cnt
      from (select Lac_No, Eff_Roi_Start_Dt, Revision_Id
              from Lacs.File_Arm_Rate_History@Dbloans
             where rownum < 1000);

    select Lac_No           as lac_no,
           Revision_Id      as col_num,
           null             as col_char,
           Eff_Roi_Start_Dt as col_date
      bulk collect into g_farh_data
      from Lacs.File_Arm_Rate_History@Dbloans
     where rownum < 50;

    dbms_output.put_line('[INSERT noop] Mis.Farh_1 -- would insert ' ||
                         v_farh1_cnt || ' rows; captured ' ||
                         g_farh_data.count || ' samples');
    dbms_output.put_line('[STATS noop] Mis.Farh_1');
    commit;

    -- Original step B: INSERT INTO Mis.Farh_2
    --   SELECT Lac_No, MAX(Eff_Roi_Start_Dt), MAX(Revision_Id)
    --     FROM Mis.Farh_1 h
    --    WHERE h.Eff_Roi_Start_Dt <
    --          (SELECT MAX(Eff_Roi_Start_Dt) FROM Mis.Farh_1
    --            WHERE Lac_No = h.Lac_No)
    --    GROUP BY h.Lac_No;
    --
    -- In the test variant, "Mis.Farh_1" is replaced by the package collection
    -- g_farh_data that we just populated.  We count and emit samples.
    v_farh2_cnt := g_farh_data.count;  -- simplistic proxy for GROUP BY count

    dbms_output.put_line('[INSERT noop] Mis.Farh_2 -- derived from g_farh_data: ' ||
                         v_farh2_cnt || ' rows');
    dbms_output.put_line('[STATS noop] Mis.Farh_2');

    log_status_ok('POP_FARH_TEST');
    commit;

  exception
    when others then
      rollback;
      v_Err := sqlerrm;
      log_status_fail('POP_FARH_TEST', v_Err);
      commit;
  end p_Pop_Farh_Test;


  -- ===========================================================================
  -- Proc 5/8 : p_Pop_Classification_Test
  -- Source : Oracle_psql.sql :: excerpt from p_Pop_Lac_Mis_Arch2 (NPA classification)
  -- Pattern: DECODE + simple INSERT SELECT + hint usage (in comment)
  -- ===========================================================================
  procedure p_Pop_Classification_Test is
    v_class_cnt number := 0;
  begin
    log_status_start('POP_CLASSIFICATION_TEST');
    commit;

    dbms_output.put_line('[TRUNCATE noop] mis.lac_acc_classification');

    -- Original: INSERT /*+ APPEND */ INTO Mis.Lac_Acc_Classification
    --            SELECT Lac_No, Class_Code, Specific_Value
    --              FROM Lacs.Lac_Acc_Classification@Dbloans;
    select count(*)
      into v_class_cnt
      from (select Lac_No,
                   Class_Code         as property_class_code,
                   Specific_Value     as realisable_value,
                   decode(Class_Code,
                          'STANDARD',   1,
                          'NPA-SUB',    2,
                          'NPA-DOUBT',  3,
                          'NPA-LOSS',   4,
                                        0) as class_rank
              from Lacs.Lac_Acc_Classification@Dbloans
             where rownum < 1000);

    select Lac_No           as lac_no,
           Specific_Value   as col_num,
           Class_Code       as col_char,
           null             as col_date
      bulk collect into g_class_data
      from Lacs.Lac_Acc_Classification@Dbloans
     where rownum < 50;

    dbms_output.put_line('[INSERT noop] Mis.Lac_Acc_Classification -- would insert ' ||
                         v_class_cnt || ' rows; captured ' ||
                         g_class_data.count || ' samples');

    for i in 1 .. least(g_class_data.count, 3) loop
      dbms_output.put_line('  sample ' || i || ': lac_no=' ||
                           g_class_data(i).lac_no ||
                           ' class=' || g_class_data(i).col_char);
    end loop;

    dbms_output.put_line('[STATS noop] Mis.Lac_Acc_Classification');
    commit;

    log_status_ok('POP_CLASSIFICATION_TEST');
    commit;

  exception
    when others then
      rollback;
      v_Err := sqlerrm;
      log_status_fail('POP_CLASSIFICATION_TEST', v_Err);
      commit;
  end p_Pop_Classification_Test;


  -- ===========================================================================
  -- Proc 6/8 : p_Pop_Nii_Daily_Prv_Test
  -- Source : Oracle_psql.sql :: p_Pop_Nii_Daily_Prv (lines 4176-4435)
  -- Pattern: Aggregate over interest rate history; NVL + SUM
  -- ===========================================================================
  procedure p_Pop_Nii_Daily_Prv_Test is
    v_nii_cnt number := 0;
  begin
    log_status_start('POP_NII_DAILY_PRV_TEST');
    commit;

    dbms_output.put_line('[TRUNCATE noop] mis.nii_current_month_stage');
    dbms_output.put_line('[TRUNCATE noop] mis.nii_daily_prv');

    -- Original: INSERT INTO Mis.Nii_Current_Month_Stage
    --           SELECT Mthyr, Origin_Branch,
    --                  SUM(NVL(Hdfc_Prin_Os,0)),
    --                  SUM(NVL(Hdfc_Prin_Os,0) * NVL(Roi,0)) / SUM(NVL(Hdfc_Prin_Os,0)),
    --                  ...
    --             FROM Mis.Nii_Current_Month_Temp
    --            GROUP BY Mthyr, Origin_Branch;
    --
    -- In the read-only variant, instead of reading from Mis.Nii_Current_Month_Temp
    -- (which does not exist), we aggregate over Lacs.Lac_Int_Reset_Trans@Dbloans
    -- as a representative real source table.

    select count(*)
      into v_nii_cnt
      from (select Lac_No,
                   sum(nvl(Revision_Id, 0))   as sum_rev_id,
                   count(*)                   as rev_count,
                   max(Eff_Roi_Start_Dt)      as max_roi_dt
              from Lacs.Lac_Int_Reset_Trans@Dbloans
             where rownum < 1000
             group by Lac_No);

    select Lac_No             as lac_no,
           count(*)           as col_num,
           'NII'              as col_char,
           max(Eff_Roi_Start_Dt) as col_date
      bulk collect into g_nii_data
      from Lacs.Lac_Int_Reset_Trans@Dbloans
     where rownum < 50
     group by Lac_No;

    dbms_output.put_line('[INSERT noop] Mis.Nii_Current_Month_Stage -- would insert ' ||
                         v_nii_cnt || ' groups; captured ' ||
                         g_nii_data.count || ' samples');

    -- Loop pattern: simulates "INSERT INTO Mis.Nii_Daily_Prv SELECT * FROM stage"
    -- from the original (line 4408).  Here we simply iterate the collection.
    for i in 1 .. g_nii_data.count loop
      null;  -- no-op iteration; would have been a row write in the original
    end loop;
    dbms_output.put_line('[INSERT noop] Mis.Nii_Daily_Prv -- iterated ' ||
                         g_nii_data.count || ' rows from stage collection');

    dbms_output.put_line('[STATS noop] Mis.Nii_Current_Month_Stage');
    dbms_output.put_line('[STATS noop] Mis.Nii_Daily_Prv');

    log_status_ok('POP_NII_DAILY_PRV_TEST');
    commit;

  exception
    when others then
      rollback;
      v_Err := sqlerrm;
      log_status_fail('POP_NII_DAILY_PRV_TEST', v_Err);
      commit;
  end p_Pop_Nii_Daily_Prv_Test;


  -- ===========================================================================
  -- Proc 7/8 : p_Upd_Wo_In_Archive_Test
  -- Source : Oracle_psql.sql :: p_Upd_Wo_In_Archive (lines 6043-6145)
  -- Pattern: UPDATE Mis.Lac_Mis_Archive SET col = subquery WHERE ...
  -- ===========================================================================
  procedure p_Upd_Wo_In_Archive_Test is
    v_update_cnt number := 0;
  begin
    log_status_start('UPD_WO_IN_ARCHIVE_TEST');
    commit;

    -- Original: UPDATE Mis.Lac_Mis_Archive a
    --              SET a.Loan_Closure_Date = (SELECT MAX(trans_dt)
    --                                           FROM Lacs.Lac_Trans@Dbloans
    --                                          WHERE lac_no = a.lac_no
    --                                            AND trans_type = 'WO')
    --            WHERE a.Yyyymm = i_Yyyymm;
    --
    -- Read-only variant: count how many rows WOULD have been updated.  We
    -- replace "Mis.Lac_Mis_Archive" with the package collection g_prin_data
    -- populated by p_Pop_Prin_Data_Test.

    select count(*)
      into v_update_cnt
      from (select l.Lac_No
              from slacs.lac_master@Dbloans l
             where rownum < 500
               and exists (select 1
                             from Lacs.Lac_Trans@Dbloans t
                            where t.lac_no = l.Lac_No
                              and t.trans_type = 'WO'));

    dbms_output.put_line('[UPDATE noop] Mis.Lac_Mis_Archive -- would update ' ||
                         v_update_cnt || ' Loan_Closure_Date rows (WO)');

    commit;
    log_status_ok('UPD_WO_IN_ARCHIVE_TEST');
    commit;

  exception
    when others then
      rollback;
      v_Err := sqlerrm;
      log_status_fail('UPD_WO_IN_ARCHIVE_TEST', v_Err);
      commit;
  end p_Upd_Wo_In_Archive_Test;


  -- ===========================================================================
  -- Proc 8/8 : Pop_Lac_Mis_Archive_Final_Test
  -- Source : Oracle_psql.sql :: Pop_Lac_Mis_Archive_Final (lines 6147-6367)
  -- Pattern: DBMS_STATS gather + DBMS_SESSION.SLEEP + DBMS_SCHEDULER finalize
  -- ===========================================================================
  procedure Pop_Lac_Mis_Archive_Final_Test is
    v_never_true boolean := false;
    v_total_procs number := 0;
  begin
    log_status_start('POP_LAC_MIS_ARCHIVE_FINAL_TEST');
    commit;

    -- Original: multiple Dbms_Stats.Gather_Table_Stats calls
    dbms_output.put_line('[STATS noop] Mis.Lac_Mis_Archive');
    dbms_output.put_line('[STATS noop] Mis.Lac_Mis_Archive_Extension');
    dbms_output.put_line('[STATS noop] Mis.Lac_Mis_Archive_Daily_Dpd');

    -- Simple count against a real table so SYSDATE/SYSTIMESTAMP and the
    -- counted-rows pattern survive into the PySpark conversion.
    select count(*)
      into v_total_procs
      from slacs.lac_master@Dbloans
     where rownum < 100;
    dbms_output.put_line('[SUMMARY] Observed ' || v_total_procs ||
                         ' master rows (sample scan)');

    -- Original: DBMS_SCHEDULER.CREATE_JOB schedules the next run.
    -- Test variant: wrapped in IF FALSE so the scheduler is never invoked.
    if v_never_true then
      DBMS_SCHEDULER.CREATE_JOB (
         JOB_NAME            => 'JOB_LAC_MIS_ARCHIVE_NEXT_TEST',
         JOB_TYPE            => 'PLSQL_BLOCK',
         JOB_ACTION          => 'begin Pkg_Lac_Mis_Archive.Pop_Lac_Mis_Archive_Final; end;',
         NUMBER_OF_ARGUMENTS => 0,
         START_DATE          => sysdate + 1,
         ENABLED             => TRUE,
         AUTO_DROP           => TRUE,
         COMMENTS            => 'SCHEDULED RUN (TEST NO-OP)');
    end if;
    dbms_output.put_line('[SCHEDULER noop] JOB_LAC_MIS_ARCHIVE_NEXT_TEST');

    commit;
    log_status_ok('POP_LAC_MIS_ARCHIVE_FINAL_TEST');
    commit;

  exception
    when others then
      rollback;
      v_Err := sqlerrm;
      log_status_fail('POP_LAC_MIS_ARCHIVE_FINAL_TEST', v_Err);
      commit;
  end Pop_Lac_Mis_Archive_Final_Test;


  -- ===========================================================================
  -- Public entry point : run_all
  -- Invokes every private test procedure in order.  This is the ONLY proc
  -- declared in the package spec.
  -- ===========================================================================
  procedure run_all is
  begin
    dbms_output.put_line('=== BEGIN: Pkg_Lac_Mis_Archive_Test.run_all ===');
    p_Sample_Test;
    p_Pop_Prin_Data_Test;
    p_Pop_Lac_Fu_Test;
    p_Pop_Farh_Test;
    p_Pop_Classification_Test;
    p_Pop_Nii_Daily_Prv_Test;
    p_Upd_Wo_In_Archive_Test;
    Pop_Lac_Mis_Archive_Final_Test;
    dbms_output.put_line('=== END: Pkg_Lac_Mis_Archive_Test.run_all ===');
  end run_all;

end Pkg_Lac_Mis_Archive_Test;
/


-- =============================================================================
-- Driver anonymous block -- runs every procedure end-to-end, so the whole
-- pipeline can be smoke-tested in one SQL*Plus / sqlcl invocation.
-- This block is PURELY for Oracle-side execution; the migration accelerator
-- treats it as a free-standing PL/SQL block and it does not disturb the
-- 8 procedures above.
-- =============================================================================
begin
  dbms_output.enable(null);   -- unlimited DBMS_OUTPUT buffer
  dbms_output.put_line('=== BEGIN: Oracle_psql_test.sql driver ===');

  p_call_Pop_Lac_Mis_Arch1_Test;
  Pkg_Lac_Mis_Archive_Test.run_all;

  dbms_output.put_line('=== END: Oracle_psql_test.sql driver ===');
end;
/
