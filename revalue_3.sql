SELECT gcc.code_combination_id,
       sob2.currency_code psob_curr,
       gcc.segment4 account,
       (nvl(gb.begin_balance_dr,
            0) - nvl(gb.begin_balance_cr,
                      0)) +
       (nvl(gb.period_net_dr,
            0) - nvl(gb.period_net_cr,
                      0)) r_accounted_ytd,
       (nvl(gb2.begin_balance_dr,
            0) - nvl(gb2.begin_balance_cr,
                      0)) +
       (nvl(gb2.period_net_dr,
            0) - nvl(gb2.period_net_cr,
                      0)) p_accounted_ytd
  FROM gl_balances          gb,
       gl_code_combinations gcc,
       gl_sets_of_books     sob,
       gl_sets_of_books     sob2,
       gl_balances          gb2
 WHERE gcc.code_combination_id = 4571
   AND gb.code_combination_id = gcc.code_combination_id
   AND gb.currency_code = sob.currency_code
   AND gb.set_of_books_id = sob.set_of_books_id
   AND sob.set_of_books_id = 3002
   AND gb.period_name = 'FEB-12'
   AND gb2.code_combination_id = gcc.code_combination_id
   AND gb2.currency_code = sob2.currency_code
   AND gb2.set_of_books_id = sob2.set_of_books_id
   AND sob2.set_of_books_id = 3001
   AND gb2.period_name = 'FEB-12';

SELECT gcc.code_combination_id,
       gb.currency_code,
       gcc.segment4 account,
       (nvl(begin_balance_dr,
            0) - nvl(begin_balance_cr,
                      0) +
       (nvl(period_net_dr,
             0) - nvl(period_net_cr,
                        0))) entered_ytd,
       (nvl(begin_balance_dr_beq,
            0) - nvl(begin_balance_cr_beq,
                      0)) +
       (nvl(period_net_dr_beq,
            0) - nvl(period_net_cr_beq,
                      0)) accounted_ytd
  FROM gl_balances          gb,
       gl_code_combinations gcc,
       gl_sets_of_books     sob,
       vld_ccl_accounts_lov ccl
 WHERE gb.code_combination_id = gcc.code_combination_id
   AND gb.currency_code != sob.currency_code
   AND gb.set_of_books_id = sob.set_of_books_id
   AND sob.set_of_books_id = 3001
   AND gb.period_name = 'FEB-12'
   AND gcc.segment1 = 'GBHK01'
   AND gcc.segment2 = 'BR0407'
   AND gcc.segment3 = 'P'
   AND ((nvl(begin_balance_dr,
             0) - nvl(begin_balance_cr,
                        0) +
       (nvl(period_net_dr,
              0) - nvl(period_net_cr,
                          0))) <> 0 OR
       (nvl(begin_balance_dr_beq,
             0) - nvl(begin_balance_cr_beq,
                        0)) +
       (nvl(period_net_dr_beq,
             0) - nvl(period_net_cr_beq,
                        0)) <> 0)
   AND ccl.ccl_account = gcc.segment4
   AND ccl.remeas_value IN ('1',
                            'A')
   AND ccl.line_of_business IN ('ALL',
                                'F',
                                'I');
SELECT bus.fx_enable_flag,
       bus.cta_account,
       bus.fx_account,
       bus.fx_cc,
       bus.fx_project,
       bus.fx_reference,
       bus.fx_round_account,
       bus.export4,
       bus.enable_flag
  FROM xxrfp_shelton_bus_map bus
 WHERE bus.primary_flag = 'Y'
   AND bus.le_code != '000000'
   AND bus.me_code LIKE '%HK%'
   FOR UPDATE;

SELECT t.closing_status,
       t.end_date
  FROM gl_period_statuses t
 WHERE t.set_of_books_id = 3001
   AND t.period_name = 'FEB-12'
   AND t.application_id = 101;

SELECT DISTINCT ca.remeas_value,
                ca.line_of_business
  FROM vld_ccl_accounts_lov ca;

drop TABLE gerfp_msas.vld_ccl_accounts_lov;
truncate TABLE xxrfp.gerfp_gl_reval_staging;

CREATE TABLE gerfp_msas.vld_ccl_accounts_lov AS
  SELECT * FROM vld_ccl_accounts_lov@gbsdb;

CREATE sequence xxrfp.gerfp_gl_reval_batch_id_s;
CREATE synonym gerfp_gl_reval_batch_id_s FOR xxrfp.gerfp_gl_reval_batch_id_s;
UPDATE xxrfp_shelton_bus_map t SET t.fx_enable_flag = 'Y';

truncate TABLE gl.gl_daily_rates;
INSERT INTO gl.gl_daily_rates
  SELECT *
    FROM gl.gl_daily_rates@gbsdb
   WHERE conversion_date >= to_date('2012-1-1',
                                    'yyyy-mm-dd');

UPDATE xxrfp_shelton_bus_map t
   SET t.fx_enable_flag = 'Y',
       t.cta_account    = nvl(t.cta_account,
                              '4840510110000'),
       t.fx_account     = nvl(t.fx_account,
                              '7400010026701'),
       t.fx_cc          = 'GX9999',
       t.fx_reference   = nvl(t.fx_reference,
                              '000000'),
       t.fx_project     = nvl(t.fx_project,
                              '0000000000')

 WHERE t.le_code != '000000'
   AND t.book_type = 'P';

UPDATE gl.gl_period_statuses t
   SET t.closing_status = 'O'
 WHERE t.application_id = 101
   AND t.period_name = 'JAN-12';

SELECT a.request_id,
       a.user_concurrent_program_name,
       a.argument_text,
       a.completion_text,
       (a.actual_completion_date - a.actual_start_date) * 1440 * 60 senconds,
       a.request_id,
       a.requestor,
       a.requested_start_date,
       a.actual_start_date,
       a.actual_completion_date
  FROM fnd_conc_requests_form_v a
 WHERE a.user_concurrent_program_name IN
       ('GERFP Revalue By Ledger',
        'GERFP Remeasurement By User',
        'GERFP Translation By User')
   AND a.actual_completion_date > SYSDATE - 10
   AND a.requestor != 'HOWLET'
   AND a.request_id NOT IN (12619540,
                            '12616534',
                            '12619542',
                            '12616535');

SELECT bus.me_code,
       bus.le_code,
       bus.book_type,
       bus.fx_enable_flag,
       bus.export4,
       bus.fx_cc,
       bus.fx_project,
       bus.fx_reference,
       bus.fx_account,
       bus.cta_account
  FROM xxrfp_shelton_bus_map bus
 WHERE bus.primary_flag = 'Y'
   AND bus.le_code != '000000'
   AND bus.enable_flag = 'Y'
   AND bus.book_type = 'P'
   AND bus.cta_account IS NULL;

SELECT a.name,
       b.conversion_rule
  FROM gl_sets_of_books       a,
       gl_mc_conversion_rules b
 WHERE a.mrc_sob_type_code = 'R'
   AND a.set_of_books_id = b.reporting_set_of_books_id(+)
   AND b.je_source_name(+) = '421';

SELECT *
  FROM gl.gl_interface t
 WHERE t.user_je_source_name IN ('Remeasurement',
                                 'Translation');

SELECT sob.name                  sob,
       gcc.concatenated_segments segments,
       t.*
  FROM xxrfp.gerfp_gl_reval_staging t,
       gl_sets_of_books             sob,
       gl_code_combinations_kfv     gcc
 WHERE 1 = 1
      --   AND t.batch_id = p_batch_id
      --AND t.status = 'E'
   AND t.sob_id = sob.set_of_books_id(+)
   AND t.ccid = gcc.code_combination_id
   AND gcc.concatenated_segments LIKE 'IQIN03.Y99000.P.3310020020000%'
 ORDER BY t.batch_id DESC;

SELECT *
  FROM gerfp_tb t
 WHERE t.concatenated_segments LIKE 'ESVN01.MS0075.P.0305310020000%'
   AND t.period_name = 'JUN-12';

SELECT tb.set_of_books_id,
       tb.name,
       tb.period_name,
       tb.mrc_sob_type_code,
       bus.export4,
       tb.segment1,
       tb.segment2,
       tb.segment3,
       tb.concatenated_segments,
       '''' || tb.segment4 acc,
       tb.description,
       '''' || ccl.ccl_account,
       ccl.remeas_value,
       '''' || ccl.translate_account,
       '''' || ccl.account_to_roll_to,
       ccl.enabled_flag ccl_enable_flag,
       ccl.line_of_business,
       ccl.remeas_trans,
       ccl.controlled_account_type,
       tb.currency_code,
       tb.functional_currency,
       tb.begin_balance,
       tb.ending_balance,
       tb.period_movement,
       tb.period_movement_beq,
       tb.ending_balance_beq
  FROM gerfp_tb              tb,
       gerfp_rev_gcc_v       ccl,
       xxrfp_shelton_bus_map bus
 WHERE 1 = 1
   AND tb.mrc_sob_type_code IN ('P',
                                'R')
   AND tb.period_name = 'JUN-12'
   AND (ending_balance <> 0 OR ending_balance_beq <> 0 OR
       period_movement <> 0)
   AND tb.code_combination_id = ccl.code_combination_id
   AND bus.primary_flag = 'Y'
   AND bus.le_code != '000000'
   AND bus.me_code = tb.segment1
   AND bus.le_code = tb.segment2
   AND bus.book_type = tb.segment3
   AND bus.enable_flag = 'Y'
   AND bus.fx_enable_flag = 'Y'
   AND tb.currency_code = tb.functional_currency;

DECLARE

  v_user_id          NUMBER;
  v_req_id           NUMBER;
  v_request_complete BOOLEAN;
  v_phase            VARCHAR2(20);
  v_status           VARCHAR2(20);
  v_dev_phase        VARCHAR2(20);
  v_dev_status       VARCHAR2(20);
  v_message          VARCHAR2(200);
  v_new_req_flag     VARCHAR2(20);

BEGIN

  v_request_complete := apps.fnd_concurrent.wait_for_request(12859793,
                                                             1,
                                                             60 * 30,
                                                             v_phase,
                                                             v_status,
                                                             v_dev_phase,
                                                             v_dev_status,
                                                             v_message);

  IF v_request_complete = TRUE THEN
    dbms_output.put_line(v_phase);
  ELSE
    dbms_output.put_line('xxx' || v_status || v_message);
  END IF;
END;
