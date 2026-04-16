-- Oracle PL/SQL Migration — Curated End-to-End Test Subset
-- 5 procedures selected as a representative mix:
--   [FAST-PATH]  p_Sample                  — simple INSERT/UPDATE + exception
--   [FAST-PATH]  p_Pop_Lac_Mis_Arch3       — INSERT-SELECT, TRUNCATE, DBMS_STATS
--   [FAST-PATH]  p_Pop_Lac_Mis_Arch10      — large INSERT-SELECT (48 cols)
--   [AGENTIC]    p_Covid_Customer_Category — correlated UPDATE (single-col + MERGE)
--   [AGENTIC]    p_Upd_Wo_In_Archive       — correlated UPDATE with EXISTS (multi-table)


--Procedure 1
create or replace procedure p_call_Pop_Lac_Mis_Arch1 is

BEGIN
DBMS_SCHEDULER.CREATE_JOB (
     JOB_NAME            => 'JOB_POP_LAC_MIS_ARCH1_1TIME',
     JOB_TYPE            => 'PLSQL_BLOCK',
     JOB_ACTION          => 'begin  slacs.p_Pop_Lac_Mis_Arch1; end;',
     NUMBER_OF_ARGUMENTS => 0,
     START_DATE          => sysdate + 1/14400,
     ENABLED             => TRUE,
     AUTO_DROP           => TRUE,
     COMMENTS            => 'ONE-TIME RUN');

   commit;

END p_call_Pop_Lac_Mis_Arch1;


--Main Procedure

--Main Procedure
CREATE OR REPLACE Package Body Pkg_Lac_Mis_Archive_Test Is

  Procedure p_Sample Is
  
  Begin
  
    Insert Into Mis.Lac_Mis_Archive_Status
      (Name, Dwld_Date, Start_Date)
    Values
      ('SAMPLE', Trunc(Sysdate, 'MI'), Sysdate);
  
    Commit;
  
    Update Mis.Lac_Mis_Archive_Status
       Set Status = 'Y', End_Date = Sysdate
     Where Name = 'SAMPLE'
       And Trunc(Dwld_Date) = Trunc(Sysdate)
       And Status Is Null;
  
    Commit;
  
  Exception
    When Others Then
      Rollback;
    
      v_Err := Sqlerrm;
    
      Update Mis.Lac_Mis_Archive_Status
         Set Status = 'N', End_Date = Sysdate, Error = v_Err
       Where Name = 'SAMPLE'
         And Trunc(Dwld_Date) = Trunc(Sysdate)
         And Status Is Null;
    
      Commit;
    
  End p_Sample;

  Procedure p_Pop_Lac_Mis_Arch3 Is
  
  Begin
  
    Insert Into Mis.Lac_Mis_Archive_Status
      (Name, Dwld_Date, Start_Date)
    Values
      ('POP_LAC_MIS_ARCH3', Trunc(Sysdate, 'MI'), Sysdate);
  
    Commit;
  
    Execute Immediate 'truncate table mis.lac_fu_1';
  
    Insert Into Mis.Lac_Fu_1
      Select Lac_No,
             Substr(Action, 1, 1) Action,
             Max(Date_Of_Action) Date_Of_Action
        From Lacs.Lac_Fu_Action@Dbloans
       Where (Substr(Action, 1, 1) In ('V', 'T', 'L', 'G') Or
             Substr(Action, 1, 2) In ('IV') Or
             Substr(Action, 1, 4) In ('S138'))
       Group By Lac_No, Substr(Action, 1, 1);
  
    Commit;
  
    Dbms_Stats.Gather_Table_Stats('MIS',
                                  'LAC_FU_1',
                                  Cascade   => True,
                                  Degree    => 2);
  
    Execute Immediate 'truncate table mis.farh_1';
    Execute Immediate 'truncate table mis.farh_2';
  
    Insert Into Mis.Farh_1
      Select Lac_No,
             Eff_Roi_Start_Dt Eff_Roi_Start_Dt,
             Revision_Id      Revision_Id
        From Lacs.File_Arm_Rate_History@Dbloans;
  
    Commit;
  
    Dbms_Stats.Gather_Table_Stats('MIS',
                                  'FARH_1',
                                  Cascade => True,
                                  Degree  => 2);
  
    Insert Into Mis.Farh_2
      Select Lac_No,
             Max(h.Eff_Roi_Start_Dt) Last_Roi_Revision_Dt,
             Max(h.Revision_Id) Last_Revision_Id
        From Mis.Farh_1 h
       Where h.Eff_Roi_Start_Dt <
             (Select Max(Eff_Roi_Start_Dt)
                From Mis.Farh_1
               Where Lac_No = h.Lac_No)
       Group By h.Lac_No;
  
    Dbms_Stats.Gather_Table_Stats('MIS',
                                  'FARH_2',
                                  Cascade => True,
                                  Degree  => 2);
  
    Update Mis.Lac_Mis_Archive_Status
       Set Status = 'Y', End_Date = Sysdate
     Where Name = 'POP_LAC_MIS_ARCH3'
       And Trunc(Dwld_Date) = Trunc(Sysdate)
       And Status Is Null;
  
    Commit;
  
  Exception
    When Others Then
      Rollback;
    
      v_Err := Sqlerrm;
    
      Update Mis.Lac_Mis_Archive_Status
         Set Status = 'N', End_Date = Sysdate, Error = v_Err
       Where Name = 'POP_LAC_MIS_ARCH3'
         And Trunc(Dwld_Date) = Trunc(Sysdate)
         And Status Is Null;
    
      Commit;
    
  End p_Pop_Lac_Mis_Arch3;

  Procedure p_Pop_Lac_Mis_Arch10 Is
  
  Begin
  
    Insert Into Mis.Lac_Mis_Archive_Status
      (Name, Dwld_Date, Start_Date)
    Values
      ('POP_LAC_MIS_ARCH10', Trunc(Sysdate, 'MI'), Sysdate);
  
    Commit;
  
    Execute Immediate 'Truncate Table Mis.Lac_Paydet_Dpd_Mth';
    Insert Into Mis.Lac_Paydet_Dpd_Mth
      (Lac_No,
       Normal_Pay_Mode,
       Normal_Pay_Thro,
       Last_Pay_Mode,
       Last_Pay_Thro,
       Paymode_Nos,
       Chq_Bnc_Nos,
       Upd_Date,
       Row_Srno,
       Emi_Recd_Unposted,
       Emi_Os_Unposted,
       Pmi_Recd_Unposted,
       Pmi_Os_Unposted,
       Si_Recd_Unposted,
       Si_Os_Unposted,
       Ai_Recd_Unposted,
       Ai_Os_Unposted,
       Mr_Recd_Unposted,
       Mr_Os_Unposted,
       Os_Months_Emi_Unposted,
       Os_Months_Pmi_Unposted,
       Prin_Last_Tr_Unposted,
       Ptp_Amt,
       Ptp_Dt,
       Ptp_Recd,
       Unmoved_Pdc_Amt,
       Pdc_Cnt,
       Das_Cnt,
       Bsi_Cnt,
       Dcs_Cnt,
       Negamrt_Revid,
       Prin_Os_Last_Tr,
       Prin_Comp_Last_Tr,
       Int_Comp_Last_Tr,
       Last_Neg_Amrt_Revision_Id,
       Neg_Amrt_Ind,
       Das_Let_Prnt_Dt,
       Prin_Os_Last_Tr_Comb,
       Months_Os_Comb,
       Customer_Grade,
       Customer_Grade2,
       Cure_Type,
       Int_Os_Last_Tr_Comb,
       Prin_Comp_Os,
       Pipeline_Amt,
       Pipeline_Dt,
       Pipeline_Proj_Bucket,
       Pipeline_Code,
       Creation_Date,
       Ai_Os_Comb)
      Select Lac_No,
             Normal_Pay_Mode,
             Normal_Pay_Thro,
             Last_Pay_Mode,
             Last_Pay_Thro,
             Paymode_Nos,
             Chq_Bnc_Nos,
             Upd_Date,
             Row_Srno,
             Emi_Recd_Unposted,
             Emi_Os_Unposted,
             Pmi_Recd_Unposted,
             Pmi_Os_Unposted,
             Si_Recd_Unposted,
             Si_Os_Unposted,
             Ai_Recd_Unposted,
             Ai_Os_Unposted,
             Mr_Recd_Unposted,
             Mr_Os_Unposted,
             Os_Months_Emi_Unposted,
             Os_Months_Pmi_Unposted,
             Prin_Last_Tr_Unposted,
             Ptp_Amt,
             Ptp_Dt,
             Ptp_Recd,
             Unmoved_Pdc_Amt,
             Pdc_Cnt,
             Das_Cnt,
             Bsi_Cnt,
             Dcs_Cnt,
             Negamrt_Revid,
             Prin_Os_Last_Tr,
             Prin_Comp_Last_Tr,
             Int_Comp_Last_Tr,
             Last_Neg_Amrt_Revision_Id,
             Neg_Amrt_Ind,
             Das_Let_Prnt_Dt,
             Prin_Os_Last_Tr_Comb,
             Months_Os_Comb,
             Customer_Grade,
             Customer_Grade2,
             Cure_Type,
             Int_Os_Last_Tr_Comb,
             Prin_Comp_Os,
             Pipeline_Amt,
             Pipeline_Dt,
             Pipeline_Proj_Bucket,
             Pipeline_Code,
             Creation_Date,
             Ai_Os_Comb
        From Lacs.Lac_Paydet_Dpd@Dbloans;
    Commit;
  
    Dbms_Stats.Gather_Table_Stats('MIS',
                                  'LAC_PAYDET_DPD_MTH',
                                  Cascade             => True,
                                  Degree              => 10);
  
    Update Mis.Lac_Mis_Archive_Status
       Set Status = 'Y', End_Date = Sysdate
     Where Name = 'POP_LAC_MIS_ARCH10'
       And Trunc(Dwld_Date) = Trunc(Sysdate)
       And Status Is Null;
  
    Commit;
  
  Exception
    When Others Then
      Rollback;
    
      v_Err := Sqlerrm;
    
      Update Mis.Lac_Mis_Archive_Status
         Set Status = 'N', End_Date = Sysdate, Error = v_Err
       Where Name = 'POP_LAC_MIS_ARCH10'
         And Trunc(Dwld_Date) = Trunc(Sysdate)
         And Status Is Null;
    
      Commit;
    
  End p_Pop_Lac_Mis_Arch10;

  Procedure p_Covid_Customer_Category(i_Yyyymm Number) As
    Ln_Yyyymm Number;
    Lv_Err    Varchar2(4000);
  Begin
    --Updating LAC MIS from Archive ; added on 12-Aug-21 ; IT Development
    Insert Into Mis.Lac_Mis_Archive_Status
      (Name, Dwld_Date, Start_Date)
    Values
      ('P_COVID_CUSTOMER_CATEGORY', Trunc(Sysdate, 'MI'), Sysdate);
  
    Commit;
    Ln_Yyyymm := i_Yyyymm;
  
    Execute Immediate 'Truncate Table Lacs.Crm_Covid_Customer_Behaviour_Temp';
    Insert Into Lacs.Crm_Covid_Customer_Behaviour_Temp
      (Lac_No,
       File_No,
       Customer_Grade_Feb20,
       Covid_Cust_Tag_Feb20,
       Customer_Grade_Mar21,
       Covid_Cust_Tag_Mar21)
      Select Lac_No,
             File_No,
             Customer_Grade_Feb20,
             Covid_Cust_Tag_Feb20,
             Customer_Grade_Mar21,
             Covid_Cust_Tag_Mar21
        From Lacs.Crm_Covid_Customer_Behaviour;
    Commit;
  
    Execute Immediate 'Analyze Table Lacs.Crm_Covid_Customer_Behaviour_Temp Compute Statistics';
  
    Update Lacs.Crm_Covid_Customer_Behaviour_Temp a
       Set a.Customer_Grade_Curr =
           (Select Upper(Lm.Customer_Grade)
              From Mis.Lac_Mis_Archive Lm
             Where Lm.Yyyymm = Ln_Yyyymm
               And Lm.Lac_No = a.Lac_No
               And Lm.Br_No = 100);
    Commit;
  
    Update Lacs.Crm_Covid_Customer_Behaviour_Temp a
       Set a.Covid_Cust_Tag_Curr = 'CATEGORY Y: LATEST DELINQUENTS - WAVE 2'
     Where (Nvl(a.Customer_Grade_Feb20, 'X')) In
           ('NEVER IN DEFAULT',
            'ACCOUNTS PAID AFTER DUE DATE WITHIN A MONTH',
            'CURRENTLY IN DEFAULT FOR THE FIRST TIME')
       And (Nvl(a.Customer_Grade_Mar21, 'X')) In
           ('NEVER IN DEFAULT',
            'ACCOUNTS PAID AFTER DUE DATE WITHIN A MONTH',
            'CURRENTLY IN DEFAULT FOR THE FIRST TIME')
       And (Nvl(a.Customer_Grade_Curr, 'X')) In
           ('WAS IN DEFAULT BUT NEVER NPA',
            'NPA IN LAST 12 MONTHS',
            'NPA PRIOR TO LAST 12 MONTHS');
    Commit;
  
    Update Lacs.Crm_Covid_Customer_Behaviour_Temp a
       Set a.Covid_Cust_Tag_Final = Coalesce(Covid_Cust_Tag_Curr,
                                             Covid_Cust_Tag_Mar21,
                                             Covid_Cust_Tag_Feb20),
           a.Dwld_Date            = Trunc(Sysdate);
    Commit;
  
    --Archive
    Update Mis.Lac_Mis_Archive a
       Set a.Covid_Cust_Category =
           (Select b.Covid_Cust_Tag_Final
              From Lacs.Crm_Covid_Customer_Behaviour_Temp b
             Where b.Lac_No = a.Lac_No)
     Where a.Br_No = 100
       And a.Yyyymm = Ln_Yyyymm;
    Commit;
  
    Update Mis.Lac_Mis_Archive_Status
       Set Status = 'Y', End_Date = Sysdate
     Where Name = 'P_COVID_CUSTOMER_CATEGORY'
       And Trunc(Dwld_Date) >= Trunc(Sysdate) - 1
       And Status Is Null;
  
    Commit;
  
  Exception
    When Others Then
      Rollback;
    
      Lv_Err := Sqlerrm;
    
      Update Mis.Lac_Mis_Archive_Status
         Set Status = 'N', End_Date = Sysdate, Error = v_Err
       Where Name = 'P_COVID_CUSTOMER_CATEGORY'
         And Trunc(Dwld_Date) >= Trunc(Sysdate) - 1
         And Status Is Null;
    
      Commit;
    
  End p_Covid_Customer_Category;

  Procedure p_Upd_Wo_In_Archive(i_Yyyymm Number) As
    Ln_Yyyymm Number;
    Lv_Err    Varchar2(4000);
    Lv_Eod_Dt Date;
    v_Str1    Varchar2(500);
    Ln_Day    Number;
  
  Begin
  
    Insert Into Mis.Lac_Mis_Archive_Status
      (Name, Dwld_Date, Start_Date)
    Values
      ('P_UPD_WO_IN_ARCHIVE', Trunc(Sysdate, 'MI'), Sysdate);
  
    Commit;
    Ln_Yyyymm := i_Yyyymm;
    Lv_Eod_Dt := (Trunc(Sysdate, 'Month') - 1);
  
    Update Mis.Lac_Mis_Archive Lm
       Set (Lm.Wo_Flag, Lm.Wo_Flag_Dt, Lm.Wo_Flag_Upd_By, Lm.Wo_Flag_Upd_On) =
           (Select Lm1.Wo_Flag,
                   Lm1.Wo_Flag_Dt,
                   Lm1.Wo_Flag_Upd_By,
                   Lm1.Wo_Flag_Upd_On
              From Lacs.Lac_Master@Dbloans  Lm1
             Where Lm1.Lac_No = Lm.Lac_No)
     Where Lm.Yyyymm = Ln_Yyyymm
       And Exists (Select 1
              From Lacs.Lac_Master@Dbloans  Lm2
             Where Lm2.Lac_No = Lm.Lac_No
               And Lm2.Wo_Flag Is Not Null);
  
    Commit;
  
    Update Mis.Lac_Mis_Archive Lm
       Set Lm.Lac_Status = Lm.Wo_Flag
     Where Lm.Yyyymm = Ln_Yyyymm
       And Lm.Lac_Status Is Null
       And Lm.Wo_Flag Is Not Null;
  
    Commit;
  
    Execute Immediate 'Truncate Table Mis.Tbl_Eligibility_Intg';
  
    Insert /*+Append*/
    Into Mis.Tbl_Eligibility_Intg
      (Lac_No, Gr_No, Eligibility, Dwld_Date)
      Select Lac_No,
             Gr_No,
             Decode(Instr(Lp1.Para_Value, '$' || To_Char(Lm.Gr_No) || '$'),
                    0,
                    'E',
                    'NE') Eligibility,
             Sysdate
        From (Select *
                From Lacs.Lac_Para@Dbloans
               Where Para_Code = 'NRP_STR') Lp1,
             Mis.Lac_Mis_Archive Lm
       Where Lm.Yyyymm = Ln_Yyyymm
         And Eligibility Is Null;
    Commit;
  
    Dbms_Stats.Gather_Table_Stats('MIS',
                                  'TBL_ELIGIBILITY_INTG',
                                  Cascade               => True,
                                  Degree                => 10);
  
    Update Mis.Lac_Mis_Archive a
       Set a.Eligibility =
           (Select e.Eligibility
              From Mis.Tbl_Eligibility_Intg e
             Where e.Lac_No = a.Lac_No)
     Where a.Br_No = 100
       And a.Yyyymm = Ln_Yyyymm
       And a.Eligibility Is Null
       And Exists (Select 1
              From Mis.Tbl_Eligibility_Intg r
             Where r.Lac_No = a.Lac_No);
  
    Commit;
  
    Update Mis.Lac_Mis_Archive_Status
       Set Status = 'Y', End_Date = Sysdate
     Where Name = 'P_UPD_WO_IN_ARCHIVE'
       And Trunc(Dwld_Date) >= Trunc(Sysdate) - 1
       And Status Is Null;
  
    Commit;
  
  Exception
    When Others Then
    
      Lv_Err := Sqlerrm;
    
      Update Mis.Lac_Mis_Archive_Status
         Set Status = 'N', End_Date = Sysdate, Error = v_Err
       Where Name = 'P_UPD_WO_IN_ARCHIVE'
         And Trunc(Dwld_Date) >= Trunc(Sysdate) - 1
         And Status Is Null;
    
      Commit;
    
  End p_Upd_Wo_In_Archive;

End Pkg_Lac_Mis_Archive_Test;
/