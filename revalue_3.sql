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
                            '12616535')

