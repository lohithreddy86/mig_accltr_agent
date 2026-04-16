-- Auto-generated stub source schemas + tables for run_001 (v3)
-- Catalog: lakehouse    Schemas: 19    Tables: 135

CREATE SCHEMA IF NOT EXISTS lakehouse.accounts
    WITH (location = 's3a://oracle-homeloans/accounts');
CREATE SCHEMA IF NOT EXISTS lakehouse.dayend
    WITH (location = 's3a://oracle-homeloans/dayend');
CREATE SCHEMA IF NOT EXISTS lakehouse.default
    WITH (location = 's3a://oracle-homeloans/default');
CREATE SCHEMA IF NOT EXISTS lakehouse.hdfc
    WITH (location = 's3a://oracle-homeloans/hdfc');
CREATE SCHEMA IF NOT EXISTS lakehouse.hdfc_cust
    WITH (location = 's3a://oracle-homeloans/hdfc_cust');
CREATE SCHEMA IF NOT EXISTS lakehouse.hlsil
    WITH (location = 's3a://oracle-homeloans/hlsil');
CREATE SCHEMA IF NOT EXISTS lakehouse.ilps
    WITH (location = 's3a://oracle-homeloans/ilps');
CREATE SCHEMA IF NOT EXISTS lakehouse.instaloan
    WITH (location = 's3a://oracle-homeloans/instaloan');
CREATE SCHEMA IF NOT EXISTS lakehouse.lacs
    WITH (location = 's3a://oracle-homeloans/lacs');
CREATE SCHEMA IF NOT EXISTS lakehouse.loan_recovery
    WITH (location = 's3a://oracle-homeloans/loan_recovery');
CREATE SCHEMA IF NOT EXISTS lakehouse.mis
    WITH (location = 's3a://oracle-homeloans/mis');
CREATE SCHEMA IF NOT EXISTS lakehouse.mis11g
    WITH (location = 's3a://oracle-homeloans/mis11g');
CREATE SCHEMA IF NOT EXISTS lakehouse.mis_archive
    WITH (location = 's3a://oracle-homeloans/mis_archive');
CREATE SCHEMA IF NOT EXISTS lakehouse.newsecu
    WITH (location = 's3a://oracle-homeloans/newsecu');
CREATE SCHEMA IF NOT EXISTS lakehouse.nhbaudit
    WITH (location = 's3a://oracle-homeloans/nhbaudit');
CREATE SCHEMA IF NOT EXISTS lakehouse.pac
    WITH (location = 's3a://oracle-homeloans/pac');
CREATE SCHEMA IF NOT EXISTS lakehouse.qlik
    WITH (location = 's3a://oracle-homeloans/qlik');
CREATE SCHEMA IF NOT EXISTS lakehouse.rpsde
    WITH (location = 's3a://oracle-homeloans/rpsde');
CREATE SCHEMA IF NOT EXISTS lakehouse.slacs
    WITH (location = 's3a://oracle-homeloans/slacs');
CREATE TABLE IF NOT EXISTS lakehouse.accounts.a_stdadj_detls (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.dayend.daily_tb_data (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.default.all_indexes (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.default.user_tab_partitions (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.default.v_dollar_log_history (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.default.v_partition_name (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc.all_branches (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc.first_disb_dump (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc.last_disb_dump (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc.mis_daily_disb_details (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc.orgn_units (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc.prop_valuation_archive (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.address_det (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.builder_master (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.cd_master (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.corp_brn_off (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.corp_employ_det (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.corp_mast (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.corp_mast_buss_area (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.cust_contact (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.cust_loan_relation (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.customer_id (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.file_preferred_contact (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.personal_det (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hdfc_cust.self_employ_det (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.hlsil.salesexe_master (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.approval (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.approved_by (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.cd_master (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.credit_apprsl (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.dsa_master (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.fee_transactions (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.file_subvent_det (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.loan_groups_tab (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.loan_grp_chars (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.loan_master (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.loan_master_extn (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.loan_summary (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.prop_view (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.release_mortgage (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.ilps.tech_val_det (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.instaloan.morat2_requests (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.instaloan.moratorium_requests (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.area_cd_master (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.arm_conversion (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.crm_covid_customer_behaviour (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.file_arm_rate_history (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_acc_classification (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_cure_types (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_daily_balances (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_daily_dpd_hist_new (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_daily_dpd_new (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_default_info (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_disb_all (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_field_agent_master (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_fu_action (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_fu_temp_arch_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_int_reset_trans (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_master (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_mastmain_activity (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_mastmain_hist (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_npa_asset_classification (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_npa_tracking (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_para (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_paydet (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_paydet_calc (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_paydet_dpd (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_payment_type (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_telecall_master (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_telecall_master_temp (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_trans (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_trans_all (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.lacs.lac_user_para (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.loan_recovery.covid_rescheduling_details (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.loan_recovery.lac_fu_prop_val_det (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.loan_recovery.lac_fu_prop_val_trx (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.loan_recovery.lac_telecall_supervisor (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.loan_recovery.prop_valuation_covid_adj (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.agency_channel_map_new (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.area_cd_master_temp_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.balance_pdc_count_temp_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.cibil_score_temp (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.covid_rescheduling_details_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.daily_tb_data_plt_comb (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.farh_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.farh_2 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.fee_os_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_agenct_details_arch (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_daily_temp_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_default_info_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_default_info_temp_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_fu_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_mastmain_hist_temp_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_mis (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_mis_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_mis_archive (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_mis_archive_daily_dpd (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_mis_archive_extension (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_mis_archive_status (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lac_paydet_dpd_mth (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lactr_temp (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lirt_temp_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.lm_repeaters_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.ls_app_by_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.nii_cost_of_funds_master (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.nii_current_month_stage (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.nii_current_month_temp (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.nii_daily_prv (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.nii_npa (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.pf_fee_recd_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.prin_data_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.project_no_temp_2 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis.std_id_temp_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis11g.mv_pac_project_view (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis_archive.agency_channel_map (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis_archive.lac_acc_classification (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis_archive.lac_mis_archive_status (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.mis_archive.prin_data_1 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.newsecu.secu_accs (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.nhbaudit.daily_tb_data_mth (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.nhbaudit.indv_lac_details_ (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.nhbaudit.indv_lac_details_intg (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.nhbaudit.indv_lac_details_mar18 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.nhbaudit.indv_lac_details_summary_ (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.nhbaudit.indv_lac_details_summary_mar22 (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.pac.pac_project (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.qlik.clss_claim_details (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.qlik.loan_book_summary (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.qlik.mis_range (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.qlik.mis_roi_range (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.rpsde.acc_adj_pdc (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.rpsde.acc_receipts_tr_pdc (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.rpsde.acc_tr_pdc (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.slacs.lac_master (id BIGINT, val VARCHAR);
CREATE TABLE IF NOT EXISTS lakehouse.slacs.loan_summary (id BIGINT, val VARCHAR);
