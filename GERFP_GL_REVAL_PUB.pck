CREATE OR REPLACE PACKAGE gerfp_gl_reval_pub IS

  -- Author  : HW70001208
  -- Created : 6/3/2012 9:54:31 PM
  -- Purpose : 

  c_source_translate CONSTANT VARCHAR2(50) := 'Translation';
  c_source_remeasure CONSTANT VARCHAR2(50) := 'Remeasurement';

  PROCEDURE reval(errbuf       OUT VARCHAR2,
                  retcode      OUT VARCHAR2,
                  p_batch_id   IN NUMBER,
                  p_me_code    IN VARCHAR2,
                  p_le_code    IN VARCHAR2,
                  p_book_type  IN VARCHAR2,
                  p_mrc_type   IN VARCHAR2,
                  p_period     IN VARCHAR2,
                  p_rate_date  IN VARCHAR2,
                  p_reval_type IN VARCHAR2);

  PROCEDURE remeasure_by_user(errbuf      OUT VARCHAR2,
                              retcode     OUT VARCHAR2,
                              p_me_code   IN VARCHAR2,
                              p_le_code   IN VARCHAR2,
                              p_book_type IN VARCHAR2,
                              p_sob_id    IN NUMBER,
                              p_period    IN VARCHAR2,
                              p_rate_date IN VARCHAR2);

  PROCEDURE translate_by_user(errbuf      OUT VARCHAR2,
                              retcode     OUT VARCHAR2,
                              p_me_code   IN VARCHAR2,
                              p_le_code   IN VARCHAR2,
                              p_book_type IN VARCHAR2,
                              p_sob_id    IN NUMBER,
                              p_period    IN VARCHAR2,
                              p_rate_date IN VARCHAR2);

  PROCEDURE output_report(errbuf     OUT VARCHAR2,
                          retcode    OUT VARCHAR2,
                          p_batch_id IN NUMBER);

  PROCEDURE remeasure(errbuf      OUT VARCHAR2,
                      retcode     OUT VARCHAR2,
                      p_me_code   IN VARCHAR2,
                      p_le_code   IN VARCHAR2,
                      p_book_type IN VARCHAR2,
                      p_mrc_type  IN VARCHAR2,
                      p_sob_id    IN NUMBER,
                      p_period    IN VARCHAR2,
                      p_rate_date IN VARCHAR2);

  PROCEDURE translate(errbuf      OUT VARCHAR2,
                      retcode     OUT VARCHAR2,
                      p_me_code   IN VARCHAR2,
                      p_le_code   IN VARCHAR2,
                      p_book_type IN VARCHAR2,
                      p_sob_id    IN NUMBER,
                      p_period    IN VARCHAR2,
                      p_rate_date IN VARCHAR2);

END gerfp_gl_reval_pub;
/
CREATE OR REPLACE PACKAGE BODY gerfp_gl_reval_pub IS

--effective date
-- error message
-- request log
-- performance

  -- Author  : HW70001208
  -- Created : 6/3/2012 9:54:31 PM
  -- Purpose : 

  c_debug_switch CONSTANT VARCHAR2(1) := 'Y';
  g_error EXCEPTION;
  g_skip_record_exception EXCEPTION;
  g_skip_record EXCEPTION;
  g_warning EXCEPTION;

  g_ret_sts_success CONSTANT VARCHAR2(1) := 'S';
  g_ret_sts_error   CONSTANT VARCHAR2(1) := 'E';
  g_ret_sts_warning CONSTANT VARCHAR2(1) := 'W';

  g_sub_source_std   CONSTANT VARCHAR2(100) := 'STANDARD';
  g_sub_source_777_d CONSTANT VARCHAR2(100) := '777_D'; --777 DETAIL
  g_sub_source_fx    CONSTANT VARCHAR2(100) := 'FX';
  g_sub_source_777_s CONSTANT VARCHAR2(100) := '777_S'; --777 SUMMARY

  g_record_status_ready     CONSTANT VARCHAR2(1) := 'R'; -- pass validation, to be processed
  g_record_status_error     CONSTANT VARCHAR2(1) := 'E'; -- failed t validate
  g_record_status_skip      CONSTANT VARCHAR2(1) := 'S'; -- no need to process, skip
  g_record_status_processed CONSTANT VARCHAR2(1) := 'P'; -- processed

  c_defaut_segment5  CONSTANT gl_code_combinations.segment5%TYPE := '000000';
  c_defaut_segment6  CONSTANT gl_code_combinations.segment6%TYPE := '0000000000';
  c_defaut_segment7  CONSTANT gl_code_combinations.segment7%TYPE := '000000';
  c_defaut_segment8  CONSTANT gl_code_combinations.segment8%TYPE := '000000';
  c_defaut_segment9  CONSTANT gl_code_combinations.segment9%TYPE := '000000';
  c_defaut_segment10 CONSTANT gl_code_combinations.segment10%TYPE := '0';
  c_defaut_segment11 CONSTANT gl_code_combinations.segment11%TYPE := '0';

  c_conc_date_format CONSTANT VARCHAR2(50) := 'YYYY/MM/DD HH24:MI:SS';

  g_reval_type           VARCHAR2(50);
  g_sob_id               NUMBER;
  g_psob_id              NUMBER;
  g_rsob_id              NUMBER;
  g_chart_of_accounts_id NUMBER;
  g_fx_ccid              NUMBER;
  g_cta_ccid             NUMBER;
  g_rounding_ccid        NUMBER;
  g_effective_date       DATE;
  g_threshold_amount     NUMBER;
  g_sob_currency         VARCHAR2(100);
  g_psob_currency        VARCHAR2(100);
  g_precision            NUMBER;
  g_mrc_type             VARCHAR2(100);
  g_fc                   VARCHAR2(100);
  g_me_line_of_business  VARCHAR2(100);
  g_rate_date            DATE;
  g_batch_id             NUMBER;
  g_me_code              VARCHAR2(100);
  g_le_code              VARCHAR2(100);
  g_book_type            VARCHAR2(100);
  g_period               VARCHAR2(100);

  TYPE g_staging_table_t IS TABLE OF gerfp_gl_reval_staging%ROWTYPE INDEX BY BINARY_INTEGER;

  PROCEDURE debug_message(p_message IN VARCHAR2) IS
  BEGIN
    IF c_debug_switch = 'Y' THEN
      IF fnd_global.conc_request_id = -1 THEN
        dbms_output.put_line(p_message);
      ELSE
        fnd_file.put_line(fnd_file.log,
                          p_message);
      END IF;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      fnd_file.put_line(fnd_file.log,
                        'Error occured in DEBUG_MESSAGE Procedure : ' ||
                        SQLERRM);
      dbms_output.put_line(p_message);
  END debug_message;

  FUNCTION get_rate(p_rate_date          IN DATE,
                    p_rate_type          IN VARCHAR2,
                    p_from_currency_code IN VARCHAR2,
                    p_to_currency_code   IN VARCHAR2,
                    x_message            OUT VARCHAR2,
                    x_status             OUT VARCHAR2) RETURN NUMBER IS
    l_rate NUMBER;
  
  BEGIN
    x_status := g_ret_sts_success;
  
    SELECT show_conversion_rate
      INTO l_rate
      FROM gl_daily_rates_v
     WHERE user_conversion_type = p_rate_type
       AND to_currency = p_to_currency_code
       AND conversion_date = p_rate_date
       AND from_currency = p_from_currency_code;
  
    debug_message(p_rate_type || ' rate from Currency:' ||
                  p_from_currency_code || ' to ' || p_to_currency_code ||
                  ' on ' || p_rate_date || ' is ' || l_rate);
  
    RETURN l_rate;
  
  EXCEPTION
    WHEN no_data_found THEN
    
      x_status  := g_ret_sts_error;
      x_message := 'Rate not defined. ' || p_rate_type ||
                   ' rate from Currency:' || p_from_currency_code || ' to ' ||
                   p_to_currency_code || ' on ' || p_rate_date;
      debug_message(x_message);
      RETURN 0;
    
    WHEN OTHERS THEN
      x_status  := g_ret_sts_error;
      x_message := 'Error in getting ' || p_rate_type ||
                   ' rate from Currency:' || p_from_currency_code || ' to ' ||
                   p_to_currency_code || ' on ' || p_rate_date ||
                   ', error:' || SQLERRM;
      debug_message(x_message);
      RETURN 0;
  END;

  PROCEDURE insert_staging_table(p_batch_id           IN NUMBER,
                                 p_me_code            IN VARCHAR2,
                                 p_le_code            IN VARCHAR2,
                                 p_book_type          IN VARCHAR2,
                                 p_period             IN VARCHAR2,
                                 p_effective_date     IN DATE,
                                 p_account            IN VARCHAR2,
                                 p_sob_id             IN NUMBER,
                                 p_ccid               IN NUMBER,
                                 p_currency_code      IN VARCHAR2,
                                 p_entered_ytd        IN NUMBER,
                                 p_accounted_ytd      IN NUMBER,
                                 p_variance_amount    IN NUMBER,
                                 p_remeas_type        IN VARCHAR2,
                                 p_rate_type          IN VARCHAR2,
                                 p_rate               IN NUMBER,
                                 p_accounted_dr       IN NUMBER,
                                 p_accounted_cr       IN NUMBER,
                                 p_source             IN VARCHAR2,
                                 p_sub_source         IN VARCHAR2,
                                 p_status             IN VARCHAR2,
                                 p_msg                IN VARCHAR2,
                                 p_account_777        IN VARCHAR2 DEFAULT NULL,
                                 p_account_776        IN VARCHAR2 DEFAULT NULL,
                                 p_attribute_category IN VARCHAR2 DEFAULT NULL,
                                 p_attribute1         IN VARCHAR2 DEFAULT NULL,
                                 p_attribute2         IN VARCHAR2 DEFAULT NULL,
                                 p_attribute3         IN VARCHAR2 DEFAULT NULL,
                                 p_attribute4         IN VARCHAR2 DEFAULT NULL,
                                 p_attribute5         IN VARCHAR2 DEFAULT NULL,
                                 p_attribute6         IN VARCHAR2 DEFAULT NULL,
                                 p_attribute7         IN VARCHAR2 DEFAULT NULL,
                                 p_attribute8         IN VARCHAR2 DEFAULT NULL,
                                 p_attribute9         IN VARCHAR2 DEFAULT NULL,
                                 p_attribute10        IN VARCHAR2 DEFAULT NULL,
                                 p_attribute11        IN VARCHAR2 DEFAULT NULL,
                                 p_attribute12        IN VARCHAR2 DEFAULT NULL,
                                 p_attribute13        IN VARCHAR2 DEFAULT NULL,
                                 p_attribute14        IN VARCHAR2 DEFAULT NULL,
                                 p_attribute15        IN VARCHAR2 DEFAULT NULL) IS
  BEGIN
    INSERT INTO gerfp_gl_reval_staging
      (reval_id,
       batch_id,
       SOURCE,
       sub_source,
       period,
       sob_id,
       me_code,
       le_code,
       book_type,
       effective_date,
       account,
       ccid,
       currency_code,
       remeas_type,
       rate_type,
       rate,
       entered_ytd,
       accounted_ytd,
       variance_amount,
       accounted_dr,
       accounted_cr,
       status,
       message,
       account_777,
       account_776,
       creation_date,
       created_by,
       last_updated_by,
       last_update_date,
       last_update_login,
       program_application_id,
       program_id,
       program_update_date,
       request_id,
       attribute_category,
       attribute1,
       attribute2,
       attribute3,
       attribute4,
       attribute5,
       attribute6,
       attribute7,
       attribute8,
       attribute9,
       attribute10,
       attribute11,
       attribute12,
       attribute13,
       attribute14,
       attribute15)
    VALUES
      (gerfp_gl_reval_staging_s.nextval,
       p_batch_id,
       p_source,
       p_sub_source,
       p_period,
       p_sob_id,
       p_me_code,
       p_le_code,
       p_book_type,
       p_effective_date,
       p_account,
       p_ccid,
       p_currency_code,
       p_remeas_type,
       p_rate_type,
       p_rate,
       p_entered_ytd,
       p_accounted_ytd,
       p_variance_amount,
       p_accounted_dr,
       p_accounted_cr,
       p_status,
       p_msg,
       p_account_777,
       p_account_776,
       SYSDATE,
       fnd_global.user_id,
       fnd_global.user_id,
       SYSDATE,
       fnd_global.login_id,
       fnd_global.prog_appl_id,
       fnd_global.conc_program_id,
       SYSDATE,
       fnd_global.conc_request_id,
       p_attribute_category,
       p_attribute1,
       p_attribute2,
       p_attribute3,
       p_attribute4,
       p_attribute5,
       p_attribute6,
       p_attribute7,
       p_attribute8,
       p_attribute9,
       p_attribute10,
       p_attribute11,
       p_attribute12,
       p_attribute13,
       p_attribute14,
       p_attribute15);
  END;

  PROCEDURE iface_insrt(p_group_id       IN NUMBER,
                        p_sob            IN NUMBER,
                        p_effective_date IN DATE,
                        p_cur_code       IN VARCHAR2,
                        p_category       IN VARCHAR2,
                        p_source         IN VARCHAR2,
                        p_ccid           IN NUMBER,
                        p_accounted_dr   IN NUMBER,
                        p_accounted_cr   IN NUMBER,
                        p_batch_name     IN VARCHAR2,
                        p_journal_name   IN VARCHAR2,
                        p_date_created   IN DATE,
                        p_created_by     IN NUMBER,
                        p_line_desc      IN VARCHAR2) IS
  BEGIN
  
    INSERT INTO gl_interface
      (status,
       set_of_books_id,
       accounting_date,
       currency_code,
       actual_flag,
       user_je_category_name,
       user_je_source_name,
       code_combination_id,
       entered_dr,
       entered_cr,
       accounted_dr,
       accounted_cr,
       date_created,
       created_by,
       group_id,
       reference1,
       reference4,
       reference10)
    VALUES
      ('NEW',
       p_sob,
       p_effective_date,
       p_cur_code,
       'A',
       p_category,
       p_source,
       p_ccid,
       decode(p_cur_code,
              g_sob_currency,
              decode(nvl(p_accounted_dr,
                         0),
                     0,
                     NULL,
                     p_accounted_dr),
              decode(nvl(p_accounted_dr,
                         0),
                     0,
                     NULL,
                     0)),
       decode(p_cur_code,
              g_sob_currency,
              decode(nvl(p_accounted_cr,
                         0),
                     0,
                     NULL,
                     p_accounted_cr),
              decode(nvl(p_accounted_cr,
                         0),
                     0,
                     NULL,
                     0)),
       decode(p_accounted_dr,
              0,
              NULL,
              p_accounted_dr),
       decode(p_accounted_cr,
              0,
              NULL,
              p_accounted_cr),
       p_date_created,
       p_created_by,
       p_group_id,
       p_batch_name,
       p_journal_name,
       p_line_desc);
  
  END iface_insrt;

  --initialize global variables and validate on ledger level
  PROCEDURE init_val_ledger(p_batch_id   IN NUMBER,
                            p_me_code    IN VARCHAR2,
                            p_le_code    IN VARCHAR2,
                            p_book_type  IN VARCHAR2,
                            p_mrc_type   IN VARCHAR2,
                            p_period     IN VARCHAR2,
                            p_rate_date  IN VARCHAR2,
                            p_reval_type IN VARCHAR2,
                            x_message    OUT VARCHAR2,
                            x_status     OUT VARCHAR2) IS
  
    l_p_sob_id NUMBER;
    l_r_sob_id NUMBER;
  
    l_fx_enable_flag        VARCHAR2(100);
    l_bus_enable_flag       VARCHAR2(100);
    l_cta_account           VARCHAR2(100);
    l_fx_account            VARCHAR2(100);
    l_fx_cc                 VARCHAR2(100);
    l_fx_project            VARCHAR2(100);
    l_fx_reference          VARCHAR2(100);
    l_fx_concat_segments    VARCHAR2(1000);
    l_cta_concat_segments   VARCHAR2(1000);
    l_fx_round_account      VARCHAR2(100);
    l_round_concat_segments VARCHAR2(1000);
    l_ext_precision         NUMBER;
    l_min_acct_unit         NUMBER;
    l_closing_status        VARCHAR2(100);
    l_lock_hanlder          VARCHAR2(32767);
    l_lock_result           NUMBER;
  
  BEGIN
  
    debug_message('----------------------' ||
                  'Start initialize and validate on ledger level' ||
                  '----------------------');
  
    x_status := g_ret_sts_success;
  
    debug_message('request lock');
    dbms_lock.allocate_unique('gerfp_gl_reval_pub:' || p_me_code || '.' ||
                              p_le_code || '.' || p_book_type || '.' ||
                              p_mrc_type,
                              l_lock_hanlder);
    l_lock_result := dbms_lock.request(lockhandle        => l_lock_hanlder,
                                       lockmode          => dbms_lock.x_mode,
                                       timeout           => 0,
                                       release_on_commit => FALSE);
    IF l_lock_result <> 0 THEN
      x_message := 'failed to request lock for ledger :' || p_me_code || '.' ||
                   p_le_code || '.' || p_book_type || ' IN Book-' ||
                   p_mrc_type || '. Lock result:' || l_lock_result;
      RAISE g_error;
    
    ELSE
      debug_message('got lock:' || l_lock_hanlder);
    
    END IF;
  
    g_batch_id  := p_batch_id;
    g_me_code   := p_me_code;
    g_le_code   := p_le_code;
    g_book_type := p_book_type;
    g_batch_id  := p_batch_id;
    g_period    := p_period;
  
    g_rate_date := to_date(p_rate_date,
                           c_conc_date_format);
    debug_message('Rate Date=' || g_rate_date);
  
    g_reval_type := p_reval_type;
    debug_message('Reval Type=' || g_reval_type);
    IF g_reval_type NOT IN (c_source_translate,
                            c_source_remeasure) THEN
      x_message := 'wrong Reval Type=' || g_reval_type;
      RAISE g_error;
    END IF;
  
    IF p_mrc_type NOT IN ('P',
                          'R') THEN
      x_message := 'wrong mrc_type:' || p_mrc_type;
      RAISE g_error;
    END IF;
  
    g_mrc_type := p_mrc_type;
  
    gerfp_utl.get_sob_id(p_me_code,
                         p_le_code,
                         p_book_type,
                         l_p_sob_id,
                         l_r_sob_id);
  
    g_psob_id := l_p_sob_id;
    g_rsob_id := l_r_sob_id;
  
    IF p_mrc_type IN ('P') THEN
      g_sob_id := l_p_sob_id;
    ELSIF p_mrc_type = 'R' THEN
      g_sob_id := l_r_sob_id;
    END IF;
  
    IF g_sob_id = -1 THEN
      x_message := 'failed to get SOB. ';
      RAISE g_error;
    END IF;
  
    debug_message('g_sob_id=' || g_sob_id || ';g_psob_id=' || g_psob_id ||
                  ';g_rsob_id=' || g_rsob_id);
  
    BEGIN
      SELECT sob.chart_of_accounts_id,
             sob.currency_code
        INTO g_chart_of_accounts_id,
             g_sob_currency
        FROM gl_sets_of_books sob
       WHERE sob.set_of_books_id = g_sob_id;
    
      debug_message('SOB Currency=' || g_sob_currency ||
                    ';g_chart_of_accounts_id=' || g_chart_of_accounts_id);
    
    EXCEPTION
      WHEN OTHERS THEN
        x_message := 'Error in getting SOB information, error:' || SQLERRM;
        RAISE g_error;
    END;
  
    BEGIN
      SELECT sob.chart_of_accounts_id,
             sob.currency_code
        INTO g_chart_of_accounts_id,
             g_psob_currency
        FROM gl_sets_of_books sob
       WHERE sob.set_of_books_id = g_psob_id;
    
      debug_message('PSOB Currency=' || g_psob_currency ||
                    ';g_chart_of_accounts_id=' || g_chart_of_accounts_id);
    
    EXCEPTION
      WHEN OTHERS THEN
        x_message := 'Error in getting PSOB information, error:' || SQLERRM;
        RAISE g_error;
    END;
  
    BEGIN
      SELECT bus.fx_enable_flag,
             bus.cta_account,
             bus.fx_account,
             bus.fx_cc,
             bus.fx_project,
             bus.fx_reference,
             bus.export4,
             bus.enable_flag
        INTO l_fx_enable_flag,
             l_cta_account,
             l_fx_account,
             l_fx_cc,
             l_fx_project,
             l_fx_reference,
             g_fc,
             l_bus_enable_flag
        FROM xxrfp_shelton_bus_map bus
       WHERE bus.primary_flag = 'Y'
         AND bus.le_code != '000000'
         AND bus.me_code = p_me_code
         AND bus.le_code = p_le_code
         AND bus.book_type = p_book_type;
    
      debug_message('fx_enable_flag= ' || l_fx_enable_flag ||
                    ' ;cta_account= ' || l_cta_account || ' ;fx_account= ' ||
                    l_fx_account || '; fx_cc= ' || l_fx_cc ||
                    '; fx_project= ' || l_fx_project || '; fx_reference= ' ||
                    l_fx_reference || '; functional currency= ' || g_fc ||
                    '; bus_enable_flag= ' || l_bus_enable_flag);
    
    EXCEPTION
      WHEN OTHERS THEN
        x_message := 'Error in getting BUS information, error:' || SQLERRM;
        RAISE g_error;
    END;
  
    IF nvl(l_fx_enable_flag,
           'N') != 'Y' THEN
      x_message := 'The Ledger does not enable FX program';
      RAISE g_error;
    END IF;
  
    IF nvl(l_bus_enable_flag,
           'N') != 'Y' THEN
      x_message := 'The Ledger is disabled';
      RAISE g_error;
    END IF;
  
    --validate local currency
  
    IF g_fc IS NULL
       OR g_fc NOT IN ('LC',
                       'USD') THEN
      x_message := 'Local Currency:' || g_fc || ' is invalid';
      RAISE g_error;
    END IF;
  
    --validate line of business of ME
    BEGIN
      SELECT t.me_mars_line_of_business
        INTO g_me_line_of_business
        FROM gerfp_msas.vld_ccl_me_lov t
       WHERE t.ccl_me_id = p_me_code;
    
      debug_message('line of business of ME:' || g_me_line_of_business);
    
    EXCEPTION
      WHEN no_data_found THEN
        x_message := 'ME:' || p_me_code || ' not exist in CCL';
        RAISE g_error;
      WHEN OTHERS THEN
        x_message := 'Error in getting CCL ME , error:' || SQLERRM;
        RAISE g_error;
    END;
  
    IF g_me_line_of_business IS NULL
       OR
       g_me_line_of_business NOT IN ('F',
                                     'I') THEN
      x_message := 'line of business of ME:' || g_me_line_of_business ||
                   ' is invalid';
      RAISE g_error;
    END IF;
  
    ---validation period
    BEGIN
    
      SELECT t.closing_status,
             t.end_date
        INTO l_closing_status,
             g_effective_date
        FROM gl_period_statuses t
       WHERE t.set_of_books_id = g_sob_id
         AND t.period_name = p_period
         AND t.application_id = 101;
    EXCEPTION
      WHEN OTHERS THEN
        x_message := 'Error in getting Period, error:' || SQLERRM;
        RAISE g_error;
    END;
  
    IF nvl(l_closing_status,
           'C') != 'O' THEN
      x_message := 'Period is not open';
      RAISE g_error;
    
    END IF;
  
    l_cta_concat_segments := p_me_code || '.' || p_le_code || '.' ||
                             p_book_type || '.' || l_cta_account || '.' ||
                             c_defaut_segment5 || '.' || c_defaut_segment6 || '.' ||
                             c_defaut_segment7 || '.' || c_defaut_segment8 || '.' ||
                             c_defaut_segment9 || '.' || c_defaut_segment10 || '.' ||
                             c_defaut_segment11;
  
    l_fx_concat_segments := p_me_code || '.' || p_le_code || '.' ||
                            p_book_type || '.' || l_fx_account || '.' ||
                            l_fx_cc || '.' || l_fx_project || '.' ||
                            c_defaut_segment7 || '.' || c_defaut_segment8 || '.' ||
                            l_fx_reference || '.' || c_defaut_segment10 || '.' ||
                            c_defaut_segment11;
  
    IF g_reval_type = c_source_remeasure THEN
      g_fx_ccid := fnd_flex_ext.get_ccid('SQLGL',
                                         'GL#',
                                         g_chart_of_accounts_id,
                                         to_char(trunc(SYSDATE),
                                                 'DD-MON-YYYY'),
                                         l_fx_concat_segments);
    
      IF nvl(g_fx_ccid,
             0) <= 0 THEN
      
        x_message := 'Error in getting FX CCID for segments:' ||
                     l_fx_concat_segments || '. error:' || fnd_message.get;
        RAISE g_error;
      
      END IF;
    END IF;
  
    IF g_reval_type = c_source_translate THEN
      g_cta_ccid := fnd_flex_ext.get_ccid('SQLGL',
                                          'GL#',
                                          g_chart_of_accounts_id,
                                          to_char(trunc(SYSDATE),
                                                  'DD-MON-YYYY'),
                                          l_cta_concat_segments);
    
      IF nvl(g_cta_ccid,
             0) <= 0 THEN
      
        x_message := 'Error in getting CTA CCID for segments:' ||
                     l_cta_concat_segments || '. error:' || fnd_message.get;
        RAISE g_error;
      
      END IF;
    END IF;
  
    /*    ---get round ccid
    l_round_concat_segments := p_me_code || '.' || p_le_code || '.' ||
                               p_book_type || '.' || l_fx_round_account ||
                               '.000000.0000000000.000000.000000.000000.0.0';
    
    g_rounding_ccid := fnd_flex_ext.get_ccid('SQLGL',
                                             'GL#',
                                             g_chart_of_accounts_id,
                                             to_char(trunc(SYSDATE),
                                                     'DD-MON-YYYY'),
                                             l_round_concat_segments);
    
    IF nvl(g_rounding_ccid,
           0) <= 0 THEN
    
      x_message := 'Error in ROUND CCID for segments:' ||
                   l_round_concat_segments || '. error:' || fnd_message.get;
      RAISE g_error;
    
    END IF;*/
  
    fnd_currency.get_info(g_sob_currency,
                          g_precision,
                          l_ext_precision,
                          l_min_acct_unit);
    g_threshold_amount := power(0.1,
                                g_precision);
    debug_message('currency precision:' || g_precision);
    debug_message('threshold_amount:' || g_threshold_amount);
  
  EXCEPTION
    WHEN g_error THEN
    
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure init_val_ledger. Errors:' ||
                   x_message;
      debug_message(x_message);
    WHEN OTHERS THEN
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure init_val_ledger. Errors:' ||
                   SQLERRM;
      debug_message(x_message);
    
  END;

  PROCEDURE cal_remeasure(p_ccid         IN NUMBER,
                          p_remeas_value IN VARCHAR2,
                          x_results      OUT g_staging_table_t,
                          x_message      OUT VARCHAR2,
                          x_status       OUT VARCHAR2) IS
  
    CURSOR l_data_crs IS
      SELECT gb.code_combination_id,
             gcc.segment4 account,
             gb.currency_code,
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
             gl_code_combinations gcc
       WHERE gb.code_combination_id = p_ccid
         AND gb.code_combination_id = gcc.code_combination_id
         AND gb.currency_code != g_sob_currency
         AND gb.set_of_books_id = g_sob_id
         AND gb.period_name = g_period
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
                              0)) <> 0);
  
    l_variance_amount NUMBER;
    l_reval_amount    NUMBER;
    l_rate_type       VARCHAR2(100);
    l_rate            NUMBER;
    l_error_msg       VARCHAR2(32637);
    l_status          VARCHAR2(100);
  
    l_count NUMBER;
  
  BEGIN
  
    x_status := g_ret_sts_success;
  
    FOR l_data_rec IN l_data_crs
    LOOP
    
      l_variance_amount := NULL;
      l_reval_amount    := NULL;
      l_rate_type       := NULL;
      l_rate            := NULL;
      l_error_msg       := NULL;
      l_status          := NULL;
    
      debug_message('start remeasuring Currency:' ||
                    l_data_rec.currency_code);
    
      -- get rate  
      l_rate_type := 'GAP';
      l_rate      := get_rate(g_rate_date,
                              l_rate_type,
                              l_data_rec.currency_code,
                              g_sob_currency,
                              l_error_msg,
                              l_status);
      IF l_status <> g_ret_sts_success THEN
        RAISE g_error;
      END IF;
    
      --insert revaluation for each line
    
      l_variance_amount := nvl(l_data_rec.entered_ytd,
                               0) * nvl(l_rate,
                                        0) - nvl(l_data_rec.accounted_ytd,
                                                 0);
    
      debug_message('variance_amount: ' || l_variance_amount);
    
      IF abs(nvl(l_variance_amount,
                 0)) >= nvl(g_threshold_amount,
                            0) THEN
      
        l_reval_amount := round(l_variance_amount,
                                g_precision);
      
        debug_message('rounded FX amount: ' || l_reval_amount);
        l_count := nvl(x_results.last,
                       0) + 1;
        x_results(l_count).account := l_data_rec.account;
        x_results(l_count).ccid := l_data_rec.code_combination_id;
        x_results(l_count).currency_code := l_data_rec.currency_code;
        x_results(l_count).entered_ytd := l_data_rec.entered_ytd;
        x_results(l_count).accounted_ytd := l_data_rec.accounted_ytd;
        x_results(l_count).variance_amount := l_variance_amount;
        x_results(l_count).remeas_type := p_remeas_value;
        x_results(l_count).rate_type := l_rate_type;
        x_results(l_count).rate := l_rate;
        x_results(l_count).accounted_dr := l_reval_amount;
        x_results(l_count).accounted_cr := 0;
        x_results(l_count).status := g_record_status_ready;
      
      END IF;
    
    END LOOP;
  
  EXCEPTION
    WHEN g_error THEN
    
      x_status  := g_ret_sts_error;
      x_message := l_error_msg;
      debug_message('Errors in procedure cal_remeasure. Errors:' ||
                    x_message);
    
    WHEN OTHERS THEN
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure cal_remeasure. Errors:' || SQLERRM;
      debug_message(x_message);
  END;

  PROCEDURE cal_translate(p_ccid         IN NUMBER,
                          p_remeas_value IN VARCHAR2,
                          x_results      OUT g_staging_table_t,
                          x_message      OUT VARCHAR2,
                          x_status       OUT VARCHAR2) IS
  
    CURSOR l_data_crs IS
      SELECT gcc.code_combination_id,
             sob2.currency_code psob_curr,
             gcc.segment4 account,
             ((nvl(gb.begin_balance_dr,
                   0) - nvl(gb.begin_balance_cr,
                              0)) +
             (nvl(gb.period_net_dr,
                   0) - nvl(gb.period_net_cr,
                              0))) r_accounted_ytd,
             ((nvl(gb2.begin_balance_dr,
                   0) - nvl(gb2.begin_balance_cr,
                              0)) +
             (nvl(gb2.period_net_dr,
                   0) - nvl(gb2.period_net_cr,
                              0))) p_accounted_ytd
        FROM gl_balances          gb,
             gl_code_combinations gcc,
             gl_sets_of_books     sob,
             gl_sets_of_books     sob2,
             gl_balances          gb2
       WHERE gcc.code_combination_id = p_ccid
         AND gb.code_combination_id = gcc.code_combination_id
         AND gb.currency_code = sob.currency_code
         AND gb.set_of_books_id = sob.set_of_books_id
         AND sob.set_of_books_id = g_sob_id
         AND gb.period_name = g_period
         AND gb2.code_combination_id = gcc.code_combination_id
         AND gb2.currency_code = sob2.currency_code
         AND gb2.set_of_books_id = sob2.set_of_books_id
         AND sob2.set_of_books_id = g_psob_id
         AND gb2.period_name = g_period
         AND (((nvl(gb.begin_balance_dr,
                    0) - nvl(gb.begin_balance_cr,
                                0)) +
             (nvl(gb.period_net_dr,
                    0) - nvl(gb.period_net_cr,
                                0))) <> 0 OR
             ((nvl(gb2.begin_balance_dr,
                    0) - nvl(gb2.begin_balance_cr,
                                0)) +
             (nvl(gb2.period_net_dr,
                    0) - nvl(gb2.period_net_cr,
                                0))) <> 0);
  
    l_variance_amount NUMBER;
    l_reval_amount    NUMBER;
    l_rate_type       VARCHAR2(100);
    l_rate            NUMBER;
    l_error_msg       VARCHAR2(32637);
    l_status          VARCHAR2(100);
    l_count           NUMBER;
  
  BEGIN
  
    x_status := g_ret_sts_success;
  
    FOR l_data_rec IN l_data_crs
    LOOP
    
      debug_message('start translating Currency:' || l_data_rec.psob_curr);
    
      l_variance_amount := NULL;
      l_reval_amount    := NULL;
      l_rate_type       := NULL;
      l_rate            := NULL;
      l_error_msg       := NULL;
      l_status          := NULL;
    
      l_rate_type := 'GAP';
      l_rate      := get_rate(g_rate_date,
                              l_rate_type,
                              l_data_rec.psob_curr,
                              g_sob_currency,
                              l_error_msg,
                              l_status);
      IF l_status <> g_ret_sts_success THEN
        RAISE g_error;
      END IF;
    
      --insert revaluation for each line
      l_variance_amount := nvl(l_data_rec.p_accounted_ytd,
                               0) * nvl(l_rate,
                                        0) - nvl(l_data_rec.r_accounted_ytd,
                                                 0);
      debug_message('variance_amount: ' || l_variance_amount);
    
      IF abs(nvl(l_variance_amount,
                 0)) >= nvl(g_threshold_amount,
                            0) THEN
      
        l_reval_amount := round(l_variance_amount,
                                g_precision);
        debug_message('rounded FX amount: ' || l_reval_amount);
      
        l_count := nvl(x_results.last,
                       0) + 1;
        x_results(l_count).account := l_data_rec.account;
        x_results(l_count).ccid := l_data_rec.code_combination_id;
        x_results(l_count).currency_code := l_data_rec.psob_curr;
        x_results(l_count).entered_ytd := l_data_rec.p_accounted_ytd;
        x_results(l_count).accounted_ytd := l_data_rec.r_accounted_ytd;
        x_results(l_count).variance_amount := l_variance_amount;
        x_results(l_count).remeas_type := p_remeas_value;
        x_results(l_count).rate_type := l_rate_type;
        x_results(l_count).rate := l_rate;
        x_results(l_count).accounted_dr := l_reval_amount;
        x_results(l_count).accounted_cr := 0;
        x_results(l_count).status := g_record_status_ready;
      
      END IF;
    
    END LOOP;
  
  EXCEPTION
    WHEN g_error THEN
    
      x_status  := g_ret_sts_error;
      x_message := l_error_msg;
      debug_message('Errors in procedure cal_remeasure. Errors:' ||
                    x_message);
    
    WHEN OTHERS THEN
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure cal_translate. Errors:' || SQLERRM;
      debug_message(x_message);
  END;

  PROCEDURE validate_account(p_account      IN VARCHAR2,
                             x_remeas_value OUT VARCHAR2,
                             x_message      OUT VARCHAR2,
                             x_status       OUT VARCHAR2) IS
  
    l_line_of_business    VARCHAR2(100);
    l_account_enable_flag VARCHAR2(100);
    l_remeas_trans        VARCHAR2(100);
  
  BEGIN
  
    x_status := g_ret_sts_success;
    --get CCL ACCOUNT 
    BEGIN
    
      SELECT ca.remeas_value,
             ca.line_of_business,
             ca.enabled_flag,
             ca.remeas_trans
        INTO x_remeas_value,
             l_line_of_business,
             l_account_enable_flag,
             l_remeas_trans
        FROM vld_ccl_accounts_lov ca
       WHERE ca.ccl_account = p_account;
    
      debug_message('remeas_value:' || x_remeas_value ||
                    '; line_of_business of account:' || l_line_of_business ||
                    '; remeas_trans:' || l_remeas_trans ||
                    '; account_enable_flag:' || l_account_enable_flag);
    EXCEPTION
      WHEN no_data_found THEN
      
        x_message := 'Account:' || p_account || ' not exist in CCL';
        RAISE g_error;
      
      WHEN OTHERS THEN
      
        x_message := 'Error in getting CCL ACCOUNT:' || p_account ||
                     'error:' || SQLERRM;
        RAISE g_error;
      
    END;
  
    IF l_account_enable_flag <> 'Y' THEN
      x_message := 'Account:' || p_account || ' is disabled in CCL';
      RAISE g_error;
    END IF;
  
    IF NOT (l_line_of_business = 'ALL' OR
        g_me_line_of_business = l_line_of_business) THEN
    
      x_message := 'line_of_business is conflict on account level and ME level';
      RAISE g_error;
    
    END IF;
  
    IF NOT ((l_line_of_business IN ('ALL',
                                    'I') AND
        x_remeas_value IN ('A',
                                'B',
                                'C',
                                'D',
                                'E',
                                'F',
                                'G',
                                'H',
                                'I',
                                'J',
                                'K')) OR
        (l_line_of_business IN ('ALL',
                                    'F') AND
        x_remeas_value IN ('1',
                                '2',
                                '3',
                                '4',
                                '5',
                                '6',
                                '7'))) THEN
    
      x_message := 'remeas_value:' || x_remeas_value || ' is invalid';
      RAISE g_error;
    
    END IF;
  EXCEPTION
    WHEN g_error THEN
    
      debug_message('Failed to validate this account, As ' || x_message);
      x_message := x_message;
      x_status  := g_ret_sts_error;
    
    WHEN OTHERS THEN
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure validate_account. Errors:' ||
                   SQLERRM;
      debug_message(x_message);
    
  END;

  PROCEDURE cal_by_ccid(p_ccid    IN NUMBER,
                        x_results OUT g_staging_table_t,
                        x_message OUT VARCHAR2,
                        x_status  OUT VARCHAR2) IS
  
    l_remeas_value VARCHAR2(100);
    l_error_msg    VARCHAR2(32637);
    l_status       VARCHAR2(100);
  
    l_code_combination_id   NUMBER;
    l_account               VARCHAR2(100);
    l_concatenated_segments VARCHAR2(1000);
  
    CURSOR l_data_crs IS
      SELECT gcc.code_combination_id,
             gcc.segment4 account,
             gcc.concatenated_segments
        FROM gl_code_combinations_kfv gcc
       WHERE gcc.code_combination_id = p_ccid;
  
  BEGIN
    x_status := g_ret_sts_success;
  
    OPEN l_data_crs;
    FETCH l_data_crs
      INTO l_code_combination_id,
           l_account,
           l_concatenated_segments;
    CLOSE l_data_crs;
  
    debug_message('start processing  CCID:' || l_code_combination_id || ' ' ||
                  l_concatenated_segments);
  
    validate_account(p_account      => l_account,
                     x_remeas_value => l_remeas_value,
                     x_message      => l_error_msg,
                     x_status       => l_status);
    IF l_status <> g_ret_sts_success THEN
      RAISE g_error;
    END IF;
  
    --functional currency
    IF g_fc = 'LC' THEN
    
      --MRC type
      IF g_mrc_type = 'P' THEN
      
        --Remeas Value
        IF l_remeas_value IN ('A',
                              '1',
                              'E') THEN
        
          IF g_reval_type = c_source_remeasure THEN
          
            cal_remeasure(p_ccid         => l_code_combination_id,
                          p_remeas_value => l_remeas_value,
                          x_results      => x_results,
                          x_message      => l_error_msg,
                          x_status       => l_status);
          
            IF l_status != g_ret_sts_success THEN
              RAISE g_error;
            END IF;
          
          END IF;
        
          --Remeas Value
        ELSE
        
          l_error_msg := 'The Account is not in remeasurement scope. remeas_value: ' ||
                         l_remeas_value;
          RAISE g_skip_record;
        
          --Remeas Value
        END IF;
      
        --MRC type
      ELSIF g_mrc_type = 'R' THEN
      
        --Remeas Value
        IF l_remeas_value IN ('A',
                              'C',
                              'E',
                              'F',
                              'I',
                              '1',
                              '3',
                              '7') THEN
        
          IF g_reval_type = c_source_translate THEN
          
            cal_translate(p_ccid         => l_code_combination_id,
                          p_remeas_value => l_remeas_value,
                          x_results      => x_results,
                          x_message      => l_error_msg,
                          x_status       => l_status);
          
            IF l_status != g_ret_sts_success THEN
              RAISE g_error;
            END IF;
          
          END IF;
        
          --Remeas Value
        ELSE
        
          l_error_msg := 'The Account is not in translating scope. remeas_value: ' ||
                         l_remeas_value;
          RAISE g_skip_record;
        
          --Remeas Value
        END IF;
      
        --MRC type
      ELSE
      
        l_error_msg := 'MAC Type:' || g_mrc_type || ' is invalid';
        RAISE g_error;
      
      END IF;
    
      --functional currency
    ELSIF g_fc = 'USD' THEN
    
      --MRC type
      IF g_mrc_type = 'P' THEN
      
        --Remeas Value
        IF l_remeas_value IN ('A',
                              'E',
                              '1') THEN
        
          IF g_reval_type = c_source_remeasure THEN
            cal_remeasure(p_ccid         => l_code_combination_id,
                          p_remeas_value => l_remeas_value,
                          x_results      => x_results,
                          x_message      => l_error_msg,
                          x_status       => l_status);
          
            IF l_status != g_ret_sts_success THEN
              RAISE g_error;
            END IF;
          
          END IF;
        
          --Remeas Value
        ELSE
        
          l_error_msg := 'The Account is not in remeasurement scope. remeas_value: ' ||
                         l_remeas_value;
          RAISE g_skip_record;
        
          --Remeas Value
        END IF;
      
        --MRC type
      ELSIF g_mrc_type = 'R' THEN
      
        --Remeas Value
        IF l_remeas_value IN ('A',
                              'E',
                              '1') THEN
        
          IF g_reval_type = c_source_remeasure THEN
            cal_remeasure(p_ccid         => l_code_combination_id,
                          p_remeas_value => l_remeas_value,
                          x_results      => x_results,
                          x_message      => l_error_msg,
                          x_status       => l_status);
          
            IF l_status != g_ret_sts_success THEN
              RAISE g_error;
            END IF;
          
          END IF;
        
          --Remeas Value
        ELSE
        
          l_error_msg := 'The Account is not in translating scope. remeas_value: ' ||
                         l_remeas_value;
          RAISE g_skip_record;
        
          --Remeas Value
        END IF;
      
        --MRC type
      ELSE
      
        l_error_msg := 'MAC Type:' || g_mrc_type || ' is invalid';
        RAISE g_error;
      
      END IF;
    
      --functional currency
    ELSE
    
      l_error_msg := 'Local Currency:' || g_fc || ' is invalid';
      RAISE g_error;
    
      --functional currency
    END IF;
  
  EXCEPTION
  
    WHEN g_skip_record THEN
      debug_message('Skip this record, As ' || l_error_msg);
    
    WHEN g_error THEN
    
      debug_message('Failed to process this record, As ' || l_error_msg);
      x_message := l_error_msg;
      x_status  := g_ret_sts_warning;
    
    WHEN OTHERS THEN
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure cal_by_ccid. Errors:' || SQLERRM;
      debug_message(x_message);
    
  END;

  PROCEDURE cal_standard(x_message OUT VARCHAR2,
                         x_status  OUT VARCHAR2) IS
  
    TYPE l_data_crs_tp IS REF CURSOR;
    l_data_crs l_data_crs_tp;
  
    l_code_combination_id NUMBER;
    l_account             VARCHAR2(100);
  
    l_results                 g_staging_table_t;
    l_idx                     NUMBER;
    l_controlled_account_type VARCHAR2(100);
    l_error_msg               VARCHAR2(32637);
    l_status                  VARCHAR2(100);
  
  BEGIN
  
    debug_message('----------------------' || 'Start cal standard' ||
                  '----------------------');
  
    x_status := g_ret_sts_success;
  
    IF g_reval_type = c_source_remeasure THEN
    
      OPEN l_data_crs FOR
        SELECT DISTINCT gcc.code_combination_id,
                        gcc.segment4 account,
                        gcc.controlled_account_type
          FROM gerfp_rev_gcc_v gcc,
               gl_balances     gb
         WHERE gcc.segment1 = g_me_code
           AND gcc.segment2 = g_le_code
           AND gcc.segment3 = g_book_type
           AND gb.code_combination_id = gcc.code_combination_id
           AND gb.currency_code != g_sob_currency
           AND gb.set_of_books_id = g_sob_id
           AND gb.period_name = g_period
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
           AND gcc.remeas_value IN ('1',
                                    'A');
    ELSIF g_reval_type = c_source_translate THEN
    
      OPEN l_data_crs FOR
        SELECT DISTINCT gcc.code_combination_id,
                        gcc.segment4 account,
                        gcc.controlled_account_type
          FROM gerfp_rev_gcc_v  gcc,
               gl_balances      gb,
               gl_sets_of_books sob,
               gl_sets_of_books sob2,
               gl_balances      gb2
         WHERE gcc.segment1 = g_me_code
           AND gcc.segment2 = g_le_code
           AND gcc.segment3 = g_book_type
           AND gb.code_combination_id = gcc.code_combination_id
           AND gb.currency_code = sob.currency_code
           AND gb.set_of_books_id = sob.set_of_books_id
           AND sob.set_of_books_id = g_sob_id
           AND gb.period_name = g_period
           AND gb2.code_combination_id = gcc.code_combination_id
           AND gb2.currency_code = sob2.currency_code
           AND gb2.set_of_books_id = sob2.set_of_books_id
           AND sob2.set_of_books_id = g_psob_id
           AND gb2.period_name = g_period
           AND (((nvl(gb.begin_balance_dr,
                      0) - nvl(gb.begin_balance_cr,
                                  0)) +
               (nvl(gb.period_net_dr,
                      0) - nvl(gb.period_net_cr,
                                  0))) <> 0 OR
               ((nvl(gb2.begin_balance_dr,
                      0) - nvl(gb2.begin_balance_cr,
                                  0)) +
               (nvl(gb2.period_net_dr,
                      0) - nvl(gb2.period_net_cr,
                                  0))) <> 0)
           AND gcc.remeas_value IN ('1',
                                    'A',
                                    'C');
    
    END IF;
  
    <<ccid_loop>>
    LOOP
    
      BEGIN
      
        l_code_combination_id := NULL;
        l_account             := NULL;
        l_results.delete;
        l_idx                     := NULL;
        l_controlled_account_type := NULL;
        l_error_msg               := NULL;
        l_status                  := NULL;
      
        FETCH l_data_crs
          INTO l_code_combination_id,
               l_account,
               l_controlled_account_type;
        EXIT WHEN l_data_crs%NOTFOUND;
      
        IF l_controlled_account_type = 'JAN1' THEN
          l_error_msg := 'account= ' || l_account || ' is a 1/1 account';
          RAISE g_skip_record_exception;
        END IF;
      
        cal_by_ccid(p_ccid    => l_code_combination_id,
                    x_results => l_results,
                    x_message => l_error_msg,
                    x_status  => l_status);
      
        IF l_status <> g_ret_sts_success THEN
          RAISE g_skip_record_exception;
        END IF;
      
        l_idx := l_results.first;
        <<results_loop>>
        WHILE l_idx IS NOT NULL
        LOOP
        
          insert_staging_table(p_batch_id        => g_batch_id,
                               p_me_code         => g_me_code,
                               p_le_code         => g_le_code,
                               p_book_type       => g_book_type,
                               p_period          => g_period,
                               p_effective_date  => g_effective_date,
                               p_account         => l_results(l_idx).account,
                               p_sob_id          => g_sob_id,
                               p_ccid            => l_results(l_idx).ccid,
                               p_currency_code   => l_results(l_idx)
                                                    .currency_code,
                               p_entered_ytd     => l_results(l_idx)
                                                    .entered_ytd,
                               p_accounted_ytd   => l_results(l_idx)
                                                    .accounted_ytd,
                               p_variance_amount => l_results(l_idx)
                                                    .variance_amount,
                               p_remeas_type     => l_results(l_idx)
                                                    .remeas_type,
                               p_rate_type       => l_results(l_idx)
                                                    .rate_type,
                               p_rate            => l_results(l_idx).rate,
                               p_accounted_dr    => l_results(l_idx)
                                                    .accounted_dr,
                               p_accounted_cr    => 0,
                               p_source          => g_reval_type,
                               p_sub_source      => g_sub_source_std,
                               p_status          => g_record_status_ready,
                               p_msg             => NULL);
        
          l_idx := l_results.next(l_idx);
        END LOOP results_loop;
      
      EXCEPTION
        WHEN g_skip_record_exception THEN
          debug_message('Failed to process this record, As ' ||
                        l_error_msg);
        
          x_status := g_ret_sts_warning;
          insert_staging_table(p_batch_id        => g_batch_id,
                               p_me_code         => g_me_code,
                               p_le_code         => g_le_code,
                               p_book_type       => g_book_type,
                               p_period          => g_period,
                               p_effective_date  => g_effective_date,
                               p_account         => l_account,
                               p_sob_id          => g_sob_id,
                               p_ccid            => l_code_combination_id,
                               p_currency_code   => NULL,
                               p_entered_ytd     => NULL,
                               p_accounted_ytd   => NULL,
                               p_variance_amount => NULL,
                               p_remeas_type     => NULL,
                               p_rate_type       => NULL,
                               p_rate            => NULL,
                               p_accounted_dr    => NULL,
                               p_accounted_cr    => NULL,
                               p_source          => g_reval_type,
                               p_sub_source      => g_sub_source_std,
                               p_status          => g_record_status_error,
                               p_msg             => l_error_msg);
        
        WHEN OTHERS THEN
          l_error_msg := 'Errors in procedure cal_standard. Errors:' ||
                         SQLERRM;
          debug_message('Failed to process this record, As ' || SQLERRM);
        
          x_status := g_ret_sts_warning;
          insert_staging_table(p_batch_id        => g_batch_id,
                               p_me_code         => g_me_code,
                               p_le_code         => g_le_code,
                               p_book_type       => g_book_type,
                               p_period          => g_period,
                               p_effective_date  => g_effective_date,
                               p_account         => l_account,
                               p_sob_id          => g_sob_id,
                               p_ccid            => l_code_combination_id,
                               p_currency_code   => NULL,
                               p_entered_ytd     => NULL,
                               p_accounted_ytd   => NULL,
                               p_variance_amount => NULL,
                               p_remeas_type     => NULL,
                               p_rate_type       => NULL,
                               p_rate            => NULL,
                               p_accounted_dr    => NULL,
                               p_accounted_cr    => NULL,
                               p_source          => g_reval_type,
                               p_sub_source      => g_sub_source_std,
                               p_status          => g_record_status_error,
                               p_msg             => l_error_msg);
        
      END;
    
    END LOOP ccid_loop;
  
    IF x_status = g_ret_sts_warning THEN
      x_message := 'some records failed in procedure cal_standard.';
      debug_message(x_message);
    END IF;
  
  EXCEPTION
  
    WHEN OTHERS THEN
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure cal_standard. Errors:' || SQLERRM;
      debug_message(x_message);
    
  END;

  PROCEDURE cal_777(x_message OUT VARCHAR2,
                    x_status  OUT VARCHAR2) IS
  
    TYPE l_data_crs_tp IS REF CURSOR;
    l_data_crs l_data_crs_tp;
  
    l_remeas_value VARCHAR2(100);
    l_rate_type    VARCHAR2(100);
    l_rate         NUMBER;
    l_error_msg    VARCHAR2(32637);
    l_status       VARCHAR2(100);
  
    l_code_combination_id NUMBER;
    l_account             VARCHAR2(100);
  
    l_results                     g_staging_table_t;
    l_idx                         NUMBER;
    l_ccl_account                 VARCHAR2(100);
    l_account_777                 VARCHAR2(100);
    l_account_776                 VARCHAR2(100);
    l_controlled_account_type     VARCHAR2(100);
    l_rev_amount                  NUMBER;
    l_amount_777                  NUMBER;
    l_amount_776                  NUMBER;
    l_psob_amount_777             NUMBER;
    l_psob_amount_776             NUMBER;
    l_777_concat_segments         VARCHAR2(1000);
    l_777_ccid                    NUMBER;
    l_controlled_account_type_777 VARCHAR2(1000);
    l_record_status               VARCHAR2(100);
  
    CURSOR l_account_777_csr IS
      SELECT t.account_777,
             t.account_776,
             t.remeas_type,
             SUM(t.accounted_dr) fx_amount
        FROM gerfp_gl_reval_staging t
       WHERE t.batch_id = g_batch_id
         AND t.me_code = g_me_code
         AND t.le_code = g_le_code
         AND t.book_type = g_book_type
         AND t.status = '777'
         AND t.sob_id = g_sob_id
       GROUP BY t.account_777,
                t.account_776,
                t.remeas_type;
  
  BEGIN
  
    debug_message('----------------------' || 'Start cal 777' ||
                  '----------------------');
    x_status := g_ret_sts_success;
  
    IF g_reval_type = c_source_remeasure THEN
    
      OPEN l_data_crs FOR
        SELECT DISTINCT gcc.code_combination_id,
                        gcc.segment4 account,
                        gcc.ccl_account,
                        gcc.remeas_value,
                        gcc.translate_account account_777,
                        (SELECT t.account_to_roll_to
                           FROM vld_ccl_accounts_lov t
                          WHERE t.ccl_account = gcc.translate_account) account_776,
                        gcc.controlled_account_type,
                        (SELECT controlled_account_type
                           FROM vld_ccl_accounts_lov t
                          WHERE t.ccl_account = gcc.translate_account) controlled_account_type_777
          FROM gerfp_rev_gcc_v gcc,
               gl_balances     gb
         WHERE gcc.segment1 = g_me_code
           AND gcc.segment2 = g_le_code
           AND gcc.segment3 = g_book_type
           AND gb.code_combination_id = gcc.code_combination_id
           AND gb.currency_code != g_sob_currency
           AND gb.set_of_books_id = g_sob_id
           AND gb.period_name = g_period
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
           AND gcc.remeas_value IN ('E')
           AND NOT (gcc.segment5 = ('XP8888') AND
                gcc.segment6 = c_defaut_segment6 AND
                gcc.segment7 = c_defaut_segment7 AND
                gcc.segment8 = c_defaut_segment8 AND
                gcc.segment9 = c_defaut_segment9 AND
                gcc.segment10 = c_defaut_segment10 AND
                gcc.segment11 = c_defaut_segment11);
    
    ELSIF g_reval_type = c_source_translate THEN
    
      OPEN l_data_crs FOR
        SELECT DISTINCT gcc.code_combination_id,
                        gcc.segment4 account,
                        gcc.ccl_account,
                        gcc.remeas_value,
                        gcc.translate_account account_777,
                        (SELECT t.account_to_roll_to
                           FROM vld_ccl_accounts_lov t
                          WHERE t.ccl_account = gcc.translate_account) account_776,
                        gcc.controlled_account_type,
                        (SELECT controlled_account_type
                           FROM vld_ccl_accounts_lov t
                          WHERE t.ccl_account = gcc.translate_account) controlled_account_type_777
          FROM gerfp_rev_gcc_v  gcc,
               gl_balances      gb,
               gl_sets_of_books sob,
               gl_sets_of_books sob2,
               gl_balances      gb2
         WHERE gcc.segment1 = g_me_code
           AND gcc.segment2 = g_le_code
           AND gcc.segment3 = g_book_type
           AND gb.code_combination_id = gcc.code_combination_id
           AND gb.currency_code = sob.currency_code
           AND gb.set_of_books_id = sob.set_of_books_id
           AND sob.set_of_books_id = g_sob_id
           AND gb.period_name = g_period
           AND gb2.code_combination_id = gcc.code_combination_id
           AND gb2.currency_code = sob2.currency_code
           AND gb2.set_of_books_id = sob2.set_of_books_id
           AND sob2.set_of_books_id = g_psob_id
           AND gb2.period_name = g_period
           AND (((nvl(gb.begin_balance_dr,
                      0) - nvl(gb.begin_balance_cr,
                                  0)) +
               (nvl(gb.period_net_dr,
                      0) - nvl(gb.period_net_cr,
                                  0))) <> 0 OR
               ((nvl(gb2.begin_balance_dr,
                      0) - nvl(gb2.begin_balance_cr,
                                  0)) +
               (nvl(gb2.period_net_dr,
                      0) - nvl(gb2.period_net_cr,
                                  0))) <> 0)
           AND gcc.remeas_value IN ('E',
                                    'F',
                                    'I',
                                    '3',
                                    '7')
           AND NOT (gcc.segment5 = ('XP8888') AND
                gcc.segment6 = c_defaut_segment6 AND
                gcc.segment7 = c_defaut_segment7 AND
                gcc.segment8 = c_defaut_segment8 AND
                gcc.segment9 = c_defaut_segment9 AND
                gcc.segment10 = c_defaut_segment10 AND
                gcc.segment11 = c_defaut_segment11);
    
    END IF;
  
    --ccid loop
    LOOP
    
      BEGIN
      
        l_remeas_value        := NULL;
        l_rate_type           := NULL;
        l_rate                := NULL;
        l_error_msg           := NULL;
        l_status              := NULL;
        l_code_combination_id := NULL;
        l_account             := NULL;
        l_results.delete;
        l_idx                         := NULL;
        l_ccl_account                 := NULL;
        l_account_777                 := NULL;
        l_account_776                 := NULL;
        l_controlled_account_type     := NULL;
        l_rev_amount                  := NULL;
        l_amount_777                  := NULL;
        l_amount_776                  := NULL;
        l_psob_amount_777             := NULL;
        l_psob_amount_776             := NULL;
        l_777_concat_segments         := NULL;
        l_777_ccid                    := NULL;
        l_controlled_account_type_777 := NULL;
        l_record_status               := NULL;
      
        l_record_status := g_record_status_skip;
      
        FETCH l_data_crs
          INTO l_code_combination_id,
               l_account,
               l_ccl_account,
               l_remeas_value,
               l_account_777,
               l_account_776,
               l_controlled_account_type,
               l_controlled_account_type_777;
        EXIT WHEN l_data_crs%NOTFOUND;
      
        debug_message('account=' || l_account || '; remeas_value=' ||
                      l_remeas_value || '; ccl_account=' || l_ccl_account ||
                      '; controlled_account_type=' ||
                      l_controlled_account_type || '; 777_account=' ||
                      l_account_777 || '; 776_account=' || l_account_776 ||
                      '; controlled_account_type of ccl account=' ||
                      l_controlled_account_type_777);
      
        --777 account is null
        IF l_account_777 IS NULL THEN
        
          -- for type other than 7 and I
          IF l_remeas_value NOT IN ('7',
                                    'I') THEN
            l_error_msg := '777 Account not defined. Remeas_value=' ||
                           l_remeas_value;
            RAISE g_skip_record_exception;
          
            -- for type 7 and I
          ELSE
          
            IF l_controlled_account_type = 'JAN1' THEN
              l_error_msg := 'account=' || l_account || ' is a 1/1 account';
              RAISE g_skip_record_exception;
            END IF;
          
            debug_message('Reval to itself');
            l_record_status := g_record_status_ready;
          
          END IF;
        
        END IF;
      
        debug_message('status=' || l_record_status);
      
        IF l_account_777 IS NOT NULL
           AND l_controlled_account_type_777 = 'JAN1' THEN
          l_error_msg := '777 Account: ' || l_account_777 ||
                         ' is a 1/1 account';
          RAISE g_skip_record_exception;
        
        END IF;
      
        cal_by_ccid(p_ccid    => l_code_combination_id,
                    x_results => l_results,
                    x_message => l_error_msg,
                    x_status  => l_status);
      
        IF l_status <> g_ret_sts_success THEN
          RAISE g_skip_record_exception;
        END IF;
      
        l_idx := l_results.first;
        <<results_loop>>
        WHILE l_idx IS NOT NULL
        LOOP
        
          insert_staging_table(p_batch_id        => g_batch_id,
                               p_me_code         => g_me_code,
                               p_le_code         => g_le_code,
                               p_book_type       => g_book_type,
                               p_period          => g_period,
                               p_effective_date  => g_effective_date,
                               p_account         => l_results(l_idx).account,
                               p_sob_id          => g_sob_id,
                               p_ccid            => l_results(l_idx).ccid,
                               p_currency_code   => l_results(l_idx)
                                                    .currency_code,
                               p_entered_ytd     => l_results(l_idx)
                                                    .entered_ytd,
                               p_accounted_ytd   => l_results(l_idx)
                                                    .accounted_ytd,
                               p_variance_amount => l_results(l_idx)
                                                    .variance_amount,
                               p_remeas_type     => l_results(l_idx)
                                                    .remeas_type,
                               p_rate_type       => l_results(l_idx)
                                                    .rate_type,
                               p_rate            => l_results(l_idx).rate,
                               p_accounted_dr    => l_results(l_idx)
                                                    .accounted_dr,
                               p_accounted_cr    => 0,
                               p_source          => g_reval_type,
                               p_sub_source      => g_sub_source_777_d,
                               p_status          => l_record_status,
                               p_msg             => NULL,
                               p_account_777     => l_account_777,
                               p_account_776     => l_account_776);
        
          l_idx := l_results.next(l_idx);
        END LOOP results_loop;
      
      EXCEPTION
        WHEN g_skip_record_exception THEN
          debug_message('Failed to process this record, As ' ||
                        l_error_msg);
          x_status := g_ret_sts_warning;
          insert_staging_table(p_batch_id        => g_batch_id,
                               p_me_code         => g_me_code,
                               p_le_code         => g_le_code,
                               p_book_type       => g_book_type,
                               p_period          => g_period,
                               p_effective_date  => g_effective_date,
                               p_account         => l_account,
                               p_sob_id          => g_sob_id,
                               p_ccid            => l_code_combination_id,
                               p_currency_code   => NULL,
                               p_entered_ytd     => NULL,
                               p_accounted_ytd   => NULL,
                               p_variance_amount => NULL,
                               p_remeas_type     => NULL,
                               p_rate_type       => NULL,
                               p_rate            => NULL,
                               p_accounted_dr    => NULL,
                               p_accounted_cr    => NULL,
                               p_source          => g_reval_type,
                               p_sub_source      => g_sub_source_777_d,
                               p_status          => g_record_status_error,
                               p_msg             => l_error_msg);
        
        WHEN OTHERS THEN
          l_error_msg := 'Errors in procedure cal_777. Errors:' || SQLERRM;
          debug_message('Failed to process this record, As ' || SQLERRM);
        
          x_status := g_ret_sts_warning;
          insert_staging_table(p_batch_id        => g_batch_id,
                               p_me_code         => g_me_code,
                               p_le_code         => g_le_code,
                               p_book_type       => g_book_type,
                               p_period          => g_period,
                               p_effective_date  => g_effective_date,
                               p_account         => l_account,
                               p_sob_id          => g_sob_id,
                               p_ccid            => l_code_combination_id,
                               p_currency_code   => NULL,
                               p_entered_ytd     => NULL,
                               p_accounted_ytd   => NULL,
                               p_variance_amount => NULL,
                               p_remeas_type     => NULL,
                               p_rate_type       => NULL,
                               p_rate            => NULL,
                               p_accounted_dr    => NULL,
                               p_accounted_cr    => NULL,
                               p_source          => g_reval_type,
                               p_sub_source      => g_sub_source_777_d,
                               p_status          => g_record_status_error,
                               p_msg             => l_error_msg);
        
      END;
    
      --ccid loop
    END LOOP;
  
    debug_message('----------------------' || 'Start cal 777 Summary' ||
                  '----------------------');
    FOR l_account_777_rec IN l_account_777_csr
    LOOP
    
      l_remeas_value        := NULL;
      l_rate_type           := NULL;
      l_rate                := NULL;
      l_error_msg           := NULL;
      l_status              := NULL;
      l_code_combination_id := NULL;
      l_account             := NULL;
      l_results.delete;
      l_idx                         := NULL;
      l_ccl_account                 := NULL;
      l_account_777                 := NULL;
      l_account_776                 := NULL;
      l_controlled_account_type     := NULL;
      l_rev_amount                  := NULL;
      l_amount_777                  := NULL;
      l_amount_776                  := NULL;
      l_psob_amount_777             := NULL;
      l_psob_amount_776             := NULL;
      l_777_concat_segments         := NULL;
      l_controlled_account_type_777 := NULL;
      l_record_status               := NULL;
      l_777_ccid                    := 0;
    
      BEGIN
      
        debug_message(' cal 777 FX amount for Account: ' ||
                      l_account_777_rec.account_777);
      
        validate_account(p_account      => l_account_777_rec.account_777,
                         x_remeas_value => l_remeas_value,
                         x_message      => l_error_msg,
                         x_status       => l_status);
        IF l_status <> g_ret_sts_success THEN
          RAISE g_skip_record_exception;
        END IF;
      
        SELECT SUM((nvl(begin_balance_dr,
                        0) - nvl(begin_balance_cr,
                                  0) +
                   (nvl(period_net_dr,
                         0) - nvl(period_net_cr,
                                    0)))) amount_777
          INTO l_amount_777
          FROM gl_balances          gb,
               gl_code_combinations gcc
         WHERE gb.code_combination_id = gcc.code_combination_id
           AND gb.currency_code = g_sob_currency
           AND gb.set_of_books_id = g_sob_id
           AND gb.period_name = g_period
           AND gcc.segment1 = g_me_code
           AND gcc.segment2 = g_le_code
           AND gcc.segment3 = g_book_type
           AND gcc.segment4 = l_account_777_rec.account_777
           AND (gcc.segment5 = 'XP8888' OR
               (l_account_777_rec.remeas_type IN
               ('E',
                  'F',
                  '3') AND gcc.segment5 = c_defaut_segment5))
           AND gcc.segment6 = c_defaut_segment6
           AND gcc.segment7 = c_defaut_segment7
           AND gcc.segment8 = c_defaut_segment8
           AND gcc.segment9 = c_defaut_segment9
           AND gcc.segment10 = c_defaut_segment10
           AND gcc.segment11 = c_defaut_segment11;
      
        IF l_account_777_rec.account_776 IS NOT NULL THEN
          SELECT SUM((nvl(begin_balance_dr,
                          0) - nvl(begin_balance_cr,
                                    0) +
                     (nvl(period_net_dr,
                           0) - nvl(period_net_cr,
                                      0)))) amount_776
            INTO l_amount_776
            FROM gl_balances          gb,
                 gl_code_combinations gcc
           WHERE gb.code_combination_id = gcc.code_combination_id
             AND gb.currency_code = g_sob_currency
             AND gb.set_of_books_id = g_sob_id
             AND gb.period_name = g_period
             AND gcc.segment1 = g_me_code
             AND gcc.segment2 = g_le_code
             AND gcc.segment3 = g_book_type
             AND gcc.segment4 = l_account_777_rec.account_776
             AND (gcc.segment5 = 'XP8888' OR
                 (l_account_777_rec.remeas_type IN
                 ('E',
                    'F',
                    '3') AND gcc.segment5 = c_defaut_segment5))
             AND gcc.segment6 = c_defaut_segment6
             AND gcc.segment7 = c_defaut_segment7
             AND gcc.segment8 = c_defaut_segment8
             AND gcc.segment9 = c_defaut_segment9
             AND gcc.segment10 = c_defaut_segment10
             AND gcc.segment11 = c_defaut_segment11;
        END IF;
      
        IF g_reval_type = c_source_translate THEN
        
          l_rate_type := 'GAP';
          l_rate      := get_rate(g_rate_date,
                                  l_rate_type,
                                  g_psob_currency,
                                  g_sob_currency,
                                  l_error_msg,
                                  l_status);
          IF l_status <> g_ret_sts_success THEN
            RAISE g_skip_record_exception;
          END IF;
        
          SELECT SUM((nvl(begin_balance_dr,
                          0) - nvl(begin_balance_cr,
                                    0) +
                     (nvl(period_net_dr,
                           0) - nvl(period_net_cr,
                                      0)))) * l_rate amount_777
            INTO l_psob_amount_777
            FROM gl_balances          gb,
                 gl_code_combinations gcc
           WHERE gb.code_combination_id = gcc.code_combination_id
             AND gb.currency_code = g_psob_currency
             AND gb.set_of_books_id = g_psob_id
             AND gb.period_name = g_period
             AND gcc.segment1 = g_me_code
             AND gcc.segment2 = g_le_code
             AND gcc.segment3 = g_book_type
             AND gcc.segment4 = l_account_777_rec.account_777
             AND (gcc.segment5 = 'XP8888' OR
                 (l_account_777_rec.remeas_type IN
                 ('E',
                    'F',
                    '3') AND gcc.segment5 = c_defaut_segment5))
             AND gcc.segment6 = c_defaut_segment6
             AND gcc.segment7 = c_defaut_segment7
             AND gcc.segment8 = c_defaut_segment8
             AND gcc.segment9 = c_defaut_segment9
             AND gcc.segment10 = c_defaut_segment10
             AND gcc.segment11 = c_defaut_segment11;
        
          IF l_account_777_rec.account_776 IS NOT NULL THEN
            SELECT SUM((nvl(begin_balance_dr,
                            0) - nvl(begin_balance_cr,
                                      0) +
                       (nvl(period_net_dr,
                             0) - nvl(period_net_cr,
                                        0)))) * l_rate amount_776
              INTO l_psob_amount_776
              FROM gl_balances          gb,
                   gl_code_combinations gcc
             WHERE gb.code_combination_id = gcc.code_combination_id
               AND gb.currency_code = g_psob_currency
               AND gb.set_of_books_id = g_psob_id
               AND gb.period_name = g_period
               AND gcc.segment1 = g_me_code
               AND gcc.segment2 = g_le_code
               AND gcc.segment3 = g_book_type
               AND gcc.segment4 = l_account_777_rec.account_776
               AND (gcc.segment5 = 'XP8888' OR
                   (l_account_777_rec.remeas_type IN
                   ('E',
                      'F',
                      '3') AND gcc.segment5 = c_defaut_segment5))
               AND gcc.segment6 = c_defaut_segment6
               AND gcc.segment7 = c_defaut_segment7
               AND gcc.segment8 = c_defaut_segment8
               AND gcc.segment9 = c_defaut_segment9
               AND gcc.segment10 = c_defaut_segment10
               AND gcc.segment11 = c_defaut_segment11;
          END IF;
        
        END IF;
      
        debug_message('fx_amount=' || l_account_777_rec.fx_amount ||
                      '; psob_amount_777=' || l_psob_amount_777 ||
                      '; psob_amount_776=' || l_psob_amount_776 ||
                      '; sob_amount_777=' || l_amount_777 ||
                      '; sob_amount_776=' || l_amount_776);
      
        IF g_reval_type = c_source_translate THEN
          l_rev_amount := nvl(l_account_777_rec.fx_amount,
                              0) + nvl(l_psob_amount_777,
                                       0) + nvl(l_psob_amount_776,
                                                0) -
                          nvl(l_amount_777,
                              0) - nvl(l_amount_776,
                                       0);
        ELSE
        
          l_rev_amount := nvl(l_account_777_rec.fx_amount,
                              0) - nvl(l_amount_777,
                                       0) - nvl(l_amount_776,
                                                0);
        END IF;
      
        IF abs(nvl(l_rev_amount,
                   0)) >= nvl(g_threshold_amount,
                              0) THEN
        
          l_rev_amount := round(l_rev_amount,
                                g_precision);
        
          debug_message('rounded FX amount: ' || l_rev_amount);
        
          l_777_concat_segments := g_me_code || '.' || g_le_code || '.' ||
                                   g_book_type || '.' ||
                                   l_account_777_rec.account_777 || '.' ||
                                   'XP8888' || '.' || c_defaut_segment6 || '.' ||
                                   c_defaut_segment7 || '.' ||
                                   c_defaut_segment8 || '.' ||
                                   c_defaut_segment9 || '.0.0';
          l_777_ccid            := fnd_flex_ext.get_ccid('SQLGL',
                                                         'GL#',
                                                         g_chart_of_accounts_id,
                                                         to_char(trunc(SYSDATE),
                                                                 'DD-MON-YYYY'),
                                                         l_777_concat_segments);
        
          IF nvl(l_777_ccid,
                 0) <= 0 THEN
          
            l_error_msg := 'Error in getting 777 CCID for segments:' ||
                           l_777_concat_segments || '. error:' ||
                           fnd_message.get;
            RAISE g_skip_record_exception;
          
          END IF;
        
          insert_staging_table(p_batch_id           => g_batch_id,
                               p_me_code            => g_me_code,
                               p_le_code            => g_le_code,
                               p_book_type          => g_book_type,
                               p_period             => g_period,
                               p_effective_date     => g_effective_date,
                               p_account            => l_account_777_rec.account_777,
                               p_sob_id             => g_sob_id,
                               p_ccid               => l_777_ccid,
                               p_currency_code      => g_psob_currency,
                               p_entered_ytd        => NULL,
                               p_accounted_ytd      => NULL,
                               p_variance_amount    => NULL,
                               p_remeas_type        => l_account_777_rec.remeas_type,
                               p_rate_type          => NULL,
                               p_rate               => NULL,
                               p_accounted_dr       => l_rev_amount,
                               p_accounted_cr       => NULL,
                               p_source             => g_reval_type,
                               p_sub_source         => g_sub_source_777_s,
                               p_status             => g_record_status_ready,
                               p_msg                => NULL,
                               p_attribute_category => g_sub_source_777_s,
                               p_attribute1         => l_account_777_rec.fx_amount,
                               p_attribute2         => l_psob_amount_777,
                               p_attribute3         => l_psob_amount_776,
                               p_attribute4         => l_amount_777,
                               p_attribute5         => l_amount_776);
        END IF;
      
      EXCEPTION
        WHEN g_skip_record_exception THEN
          debug_message('Failed to process this record, As ' ||
                        l_error_msg);
          x_status := g_ret_sts_warning;
          insert_staging_table(p_batch_id           => g_batch_id,
                               p_me_code            => g_me_code,
                               p_le_code            => g_le_code,
                               p_book_type          => g_book_type,
                               p_period             => g_period,
                               p_effective_date     => g_effective_date,
                               p_account            => l_account_777_rec.account_777,
                               p_sob_id             => g_sob_id,
                               p_ccid               => l_777_ccid,
                               p_currency_code      => g_psob_currency,
                               p_entered_ytd        => NULL,
                               p_accounted_ytd      => NULL,
                               p_variance_amount    => NULL,
                               p_remeas_type        => l_account_777_rec.remeas_type,
                               p_rate_type          => NULL,
                               p_rate               => NULL,
                               p_accounted_dr       => l_rev_amount,
                               p_accounted_cr       => NULL,
                               p_source             => g_reval_type,
                               p_sub_source         => g_sub_source_777_s,
                               p_status             => g_record_status_error,
                               p_msg                => l_error_msg,
                               p_attribute_category => g_sub_source_777_s,
                               p_attribute1         => l_account_777_rec.fx_amount,
                               p_attribute2         => l_psob_amount_777,
                               p_attribute3         => l_psob_amount_776,
                               p_attribute4         => l_amount_777,
                               p_attribute5         => l_amount_776);
        
        WHEN OTHERS THEN
        
          l_error_msg := 'Errors in procedure cal_777. Errors:' || SQLERRM;
          debug_message('Failed to process this record, As ' || SQLERRM);
        
          x_status := g_ret_sts_warning;
          insert_staging_table(p_batch_id        => g_batch_id,
                               p_me_code         => g_me_code,
                               p_le_code         => g_le_code,
                               p_book_type       => g_book_type,
                               p_period          => g_period,
                               p_effective_date  => g_effective_date,
                               p_account         => l_account,
                               p_sob_id          => g_sob_id,
                               p_ccid            => l_code_combination_id,
                               p_currency_code   => NULL,
                               p_entered_ytd     => NULL,
                               p_accounted_ytd   => NULL,
                               p_variance_amount => NULL,
                               p_remeas_type     => NULL,
                               p_rate_type       => NULL,
                               p_rate            => NULL,
                               p_accounted_dr    => NULL,
                               p_accounted_cr    => NULL,
                               p_source          => g_reval_type,
                               p_sub_source      => g_sub_source_777_s,
                               p_status          => g_record_status_error,
                               p_msg             => l_error_msg);
        
      END;
    
    -- 777 FX LOOP
    END LOOP;
  
    IF x_status = g_ret_sts_warning THEN
      x_message := 'some records failed in procedure cal_777.';
      debug_message(x_message);
    END IF;
  
  EXCEPTION
    WHEN g_error THEN
    
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure cal_777. Errors:' || x_message;
      debug_message(x_message);
    
    WHEN OTHERS THEN
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure cal_777. Errors:' || SQLERRM;
      debug_message(x_message);
    
  END;

  PROCEDURE cal_unrealized_gl(p_batch_id  IN NUMBER,
                              p_sob_id    IN NUMBER,
                              p_me_code   IN VARCHAR2,
                              p_le_code   IN VARCHAR2,
                              p_book_type IN VARCHAR2,
                              p_period    IN VARCHAR2,
                              x_message   OUT VARCHAR2,
                              x_status    OUT VARCHAR2) IS
  
    CURSOR l_data_crs IS
      SELECT gb.code_combination_id,
             gcc.segment4 account,
             gb.currency_code,
             (nvl(period_net_dr,
                  0) - nvl(period_net_cr,
                            0)) entered_ptd,
             (nvl(period_net_dr_beq,
                  0) - nvl(period_net_cr_beq,
                            0)) accounted_ptd
        FROM gl_balances          gb,
             gl_code_combinations gcc
       WHERE gb.code_combination_id = gcc.code_combination_id
         AND gcc.segment1 = p_me_code
         AND gcc.segment2 = p_le_code
         AND gcc.segment3 = p_book_type
         AND gcc.segment4 = '7400010026708'
         AND gb.currency_code != g_sob_currency
         AND gb.set_of_books_id = g_sob_id
         AND gb.period_name = p_period
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
                              0)) <> 0);
  
    l_variance_amount NUMBER;
    l_reval_amount    NUMBER;
    l_rate_type       VARCHAR2(100);
    l_rate            NUMBER;
    l_error_msg       VARCHAR2(32637);
  
  BEGIN
  
    debug_message('----------------------' || 'Start cal 777' ||
                  '----------------------');
    x_status := g_ret_sts_success;
  
    FOR l_data_rec IN l_data_crs
    LOOP
    
      l_rate_type := 'GLMOR';
      -- get rate
      BEGIN
        /*        l_rate := get_rate(g_rate_date,
        l_rate_type,
        l_data_rec.currency_code,
        g_sob_currency);*/
      
        debug_message('rate from Currency:' || l_data_rec.currency_code ||
                      ' to ' || g_sob_currency || ' on ' || g_rate_date ||
                      ' is ' || l_rate);
      
      EXCEPTION
        WHEN OTHERS THEN
        
          l_error_msg := 'Error in getting rate from Currency:' ||
                         l_data_rec.currency_code || ' to ' ||
                         g_sob_currency || ' on ' || g_rate_date ||
                         ', error:' || SQLERRM;
          debug_message(l_error_msg);
          RAISE g_error;
      END;
    
      --insert revaluation for each line
    
      l_variance_amount := nvl(l_data_rec.entered_ptd,
                               0) * nvl(l_rate,
                                        0) - nvl(l_data_rec.accounted_ptd,
                                                 0);
    
      debug_message('variance_amount: ' || l_variance_amount);
    
      IF abs(nvl(l_variance_amount,
                 0)) >= nvl(g_threshold_amount,
                            0) THEN
      
        l_reval_amount := round(l_variance_amount,
                                g_precision);
      
        debug_message('rounded FX amount: ' || l_reval_amount);
      
        insert_staging_table(p_batch_id        => p_batch_id,
                             p_me_code         => p_me_code,
                             p_le_code         => p_le_code,
                             p_book_type       => p_book_type,
                             p_period          => p_period,
                             p_effective_date  => g_effective_date,
                             p_account         => l_data_rec.account,
                             p_sob_id          => g_sob_id,
                             p_ccid            => l_data_rec.code_combination_id,
                             p_currency_code   => l_data_rec.currency_code,
                             p_entered_ytd     => l_data_rec.entered_ptd,
                             p_accounted_ytd   => l_data_rec.accounted_ptd,
                             p_variance_amount => l_variance_amount,
                             p_remeas_type     => NULL,
                             p_rate_type       => l_rate_type,
                             p_rate            => l_rate,
                             p_accounted_dr    => l_reval_amount,
                             p_accounted_cr    => 0,
                             p_source          => g_reval_type,
                             p_sub_source      => 'UNREALIZED_GL',
                             p_status          => l_reval_amount,
                             p_msg             => NULL);
      
      END IF;
    
    END LOOP;
  
  EXCEPTION
    WHEN g_error THEN
    
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure cal_unrealized_gl. Errors:' ||
                   x_message;
      debug_message(x_message);
    
    WHEN OTHERS THEN
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure cal_unrealized_gl. Errors:' ||
                   SQLERRM;
      debug_message(x_message);
    
  END;

  PROCEDURE cal_fx(x_message OUT VARCHAR2,
                   x_status  OUT VARCHAR2) IS
    CURSOR l_data_csr IS
      SELECT t.batch_id,
             t.me_code,
             t.le_code,
             t.book_type,
             t.period,
             t.sob_id,
             t.currency_code,
             t.source,
             SUM(t.variance_amount) variance_tot,
             SUM(nvl(t.accounted_dr,
                     0) - nvl(t.accounted_cr,
                              0)) reval_tot
        FROM gerfp_gl_reval_staging t
       WHERE t.batch_id = g_batch_id
         AND t.me_code = g_me_code
         AND t.le_code = g_le_code
         AND t.book_type = g_book_type
         AND t.status = g_record_status_ready
         AND t.sob_id = g_sob_id
       GROUP BY t.batch_id,
                t.me_code,
                t.le_code,
                t.book_type,
                t.period,
                t.sob_id,
                t.currency_code,
                t.source;
  
    l_fx_amount    NUMBER;
    l_round_amount NUMBER;
    l_fx_ccid      NUMBER;
  
  BEGIN
  
    debug_message('----------------------' || 'Start cal FX' ||
                  '----------------------');
  
    FOR l_data_rec IN l_data_csr
    LOOP
    
      l_fx_amount    := NULL;
      l_round_amount := NULL;
      l_fx_ccid      := NULL;
    
      l_fx_amount := l_data_rec.reval_tot;
    
      /*      l_fx_amount    := round(l_data_rec.variance_tot,
                              g_precision);
      l_round_amount := nvl(l_fx_amount,
                            0) - nvl(l_data_rec.reval_tot,
                                     0);*/
      IF l_data_rec.source = c_source_remeasure THEN
        l_fx_ccid := g_fx_ccid;
      ELSIF l_data_rec.source = c_source_translate THEN
        l_fx_ccid := g_cta_ccid;
      END IF;
    
      IF abs(nvl(l_fx_amount,
                 0)) > 0 THEN
        insert_staging_table(p_batch_id        => l_data_rec.batch_id,
                             p_me_code         => l_data_rec.me_code,
                             p_le_code         => l_data_rec.le_code,
                             p_book_type       => l_data_rec.book_type,
                             p_period          => l_data_rec.period,
                             p_effective_date  => g_effective_date,
                             p_account         => NULL,
                             p_sob_id          => l_data_rec.sob_id,
                             p_ccid            => l_fx_ccid,
                             p_currency_code   => l_data_rec.currency_code,
                             p_entered_ytd     => NULL,
                             p_accounted_ytd   => NULL,
                             p_remeas_type     => NULL,
                             p_rate_type       => NULL,
                             p_rate            => NULL,
                             p_accounted_dr    => 0,
                             p_accounted_cr    => l_fx_amount,
                             p_status          => g_record_status_ready,
                             p_msg             => NULL,
                             p_variance_amount => l_data_rec.variance_tot,
                             p_source          => l_data_rec.source,
                             p_sub_source      => g_sub_source_fx);
      END IF;
    
    -- insert rounding ammount(DR)
    
    /*      IF abs(nvl(l_round_amount,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             0)) > 0 THEN
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    insert_staging_table(p_batch_id        => l_data_rec.batch_id,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_me_code         => l_data_rec.me_code,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_le_code         => l_data_rec.le_code,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_book_type       => l_data_rec.book_type,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_period          => l_data_rec.period,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_effective_date  => g_effective_date,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_account         => NULL,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_sob_id          => l_data_rec.sob_id,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_ccid            => g_rounding_ccid,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_currency_code   => l_data_rec.currency_code,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_entered_ytd     => NULL,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_accounted_ytd   => NULL,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_remeas_type     => NULL,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_rate_type       => NULL,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_rate            => NULL,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_accounted_dr    => l_round_amount,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_accounted_cr    => 0,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_status          => 'R',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_msg             => NULL,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_variance_amount => NULL,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         p_source          => 'FX');
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  END IF;*/
    
    END LOOP;
  
  EXCEPTION
    WHEN g_error THEN
    
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure cal_fx. Errors:' || x_message;
      debug_message(x_message);
    
    WHEN OTHERS THEN
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure cal_fx. Errors:' || SQLERRM;
      debug_message(x_message);
    
  END;

  PROCEDURE insert_gl_interface(x_group_id OUT NUMBER,
                                x_message  OUT VARCHAR2,
                                x_status   OUT VARCHAR2) IS
  
    l_category  VARCHAR2(100);
    l_source    VARCHAR2(100);
    l_line_desc VARCHAR2(4000);
  
    CURSOR l_data_csr IS
      SELECT *
        FROM gerfp_gl_reval_staging t
       WHERE t.batch_id = g_batch_id
         AND t.me_code = g_me_code
         AND t.le_code = g_le_code
         AND t.book_type = g_book_type
         AND t.status = g_record_status_ready
         AND t.sob_id = g_sob_id;
  
  BEGIN
  
    debug_message('----------------------' || 'Start insert_gl_interface' ||
                  '----------------------');
  
    x_status := g_ret_sts_success;
  
    SELECT gerfp_gl_rev_seq.nextval INTO x_group_id FROM dual;
  
    debug_message('GL Group ID:' || x_group_id);
  
    FOR l_data_rec IN l_data_csr
    LOOP
    
      IF l_data_rec.source = c_source_remeasure THEN
        l_category  := c_source_remeasure;
        l_source    := c_source_remeasure;
        l_line_desc := 'Remeasurement process by date of ' || SYSDATE ||
                       '/ FX Type:' || l_data_rec.remeas_type;
      ELSIF l_data_rec.source = c_source_translate THEN
        l_category  := c_source_translate;
        l_source    := c_source_translate;
        l_line_desc := 'Translation process by date of ' || SYSDATE ||
                       '/ FX Type:' || l_data_rec.remeas_type;
      END IF;
    
      iface_insrt(p_group_id       => x_group_id,
                  p_sob            => l_data_rec.sob_id,
                  p_effective_date => l_data_rec.effective_date, --     SYSDATE,
                  p_cur_code       => l_data_rec.currency_code,
                  p_category       => l_category,
                  p_source         => l_source,
                  p_ccid           => l_data_rec.ccid,
                  p_accounted_dr   => l_data_rec.accounted_dr,
                  p_accounted_cr   => l_data_rec.accounted_cr,
                  p_batch_name     => g_me_code || '.' || g_le_code,
                  p_journal_name   => NULL,
                  p_date_created   => SYSDATE,
                  p_created_by     => fnd_global.user_id,
                  p_line_desc      => l_line_desc);
    
    END LOOP;
  
    UPDATE gerfp_gl_reval_staging t
       SET t.status = g_record_status_processed
     WHERE t.batch_id = g_batch_id
       AND t.me_code = g_me_code
       AND t.le_code = g_le_code
       AND t.book_type = g_book_type
       AND t.status = g_record_status_ready
       AND t.sob_id = g_sob_id;
  
  EXCEPTION
    WHEN OTHERS THEN
    
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure insert_gl_interface. Errors' ||
                   SQLERRM;
      debug_message(x_message);
    
      UPDATE gerfp_gl_reval_staging t
         SET t.status  = g_ret_sts_error,
             t.message = substr(substr(t.message,
                                       1,
                                       3000) || '/' || x_message,
                                1,
                                4000)
       WHERE t.batch_id = g_batch_id
         AND t.me_code = g_me_code
         AND t.le_code = g_le_code
         AND t.book_type = g_book_type
         AND t.status = g_record_status_ready
         AND t.sob_id = g_sob_id;
    
  END;

  PROCEDURE submit_journal_import(p_sob_id      IN NUMBER,
                                  p_group_id    IN NUMBER,
                                  p_user_source IN VARCHAR2,
                                  x_message     OUT VARCHAR2,
                                  x_status      OUT VARCHAR2) IS
  
    v_req_id           NUMBER;
    v_interface_run_id NUMBER;
    v_je_source        VARCHAR2(50);
  
    l_line_count NUMBER;
  
  BEGIN
  
    debug_message('----------------------' ||
                  'Start submit_journal_import' ||
                  '----------------------');
    debug_message('group= ' || p_group_id || ' ; sob id= ' || p_sob_id ||
                  ' ; user_source= ' || p_user_source);
  
    SELECT COUNT(1)
      INTO l_line_count
      FROM gl_interface t
     WHERE t.user_je_source_name = p_user_source
       AND t.set_of_books_id = p_sob_id
       AND t.group_id = p_group_id;
  
    IF l_line_count = 0 THEN
    
      x_message := 'No lines in GL Interface for group :' || p_group_id ||
                   ';sob id=' || p_sob_id || ';user_source=' ||
                   p_user_source;
      RAISE g_warning;
    END IF;
  
    BEGIN
      SELECT je_source_name
        INTO v_je_source
        FROM gl_je_sources
       WHERE user_je_source_name = p_user_source;
    EXCEPTION
    
      WHEN OTHERS THEN
        x_message := 'Errors in getting source name for user source:' ||
                     v_je_source || '. Errors' || SQLERRM;
        RAISE g_error;
    END;
  
    SELECT gl_journal_import_s.nextval INTO v_interface_run_id FROM dual;
  
    /* Insert record to interface control table */
    INSERT INTO gl_interface_control
      (je_source_name,
       status,
       interface_run_id,
       group_id,
       set_of_books_id,
       packet_id,
       request_id)
    VALUES
      (v_je_source --
      ,
       g_ret_sts_success,
       v_interface_run_id,
       p_group_id,
       p_sob_id,
       NULL,
       fnd_global.conc_request_id);
  
    v_req_id := fnd_request.submit_request(application => 'SQLGL',
                                           program     => 'GLLEZL',
                                           description => NULL,
                                           start_time  => SYSDATE,
                                           sub_request => FALSE,
                                           argument1   => to_char(v_interface_run_id),
                                           argument2   => to_char(p_sob_id),
                                           argument3   => 'N' --Suspense Flag
                                          ,
                                           argument4   => NULL,
                                           argument5   => NULL,
                                           argument6   => 'N' -- Summary Flag
                                          ,
                                           argument7   => 'O' --Import DFF w/out validation
                                           );
  
    COMMIT;
  
    debug_message('GL Import Request id :' || v_req_id);
  
    IF v_req_id = 0 THEN
      x_message := 'Failed to submit GL Import. Errors:' || fnd_message.get;
      RAISE g_error;
    END IF;
  
  EXCEPTION
  
    WHEN g_warning THEN
      x_status := g_ret_sts_warning;
      debug_message('Warning in procedure submit_journal_import. Message:' ||
                    x_message);
    
    WHEN g_error THEN
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure submit_journal_import. please delete lines in GL Interface. Errors' ||
                   x_message;
      debug_message(x_message);
    
    WHEN OTHERS THEN
      x_status  := g_ret_sts_error;
      x_message := 'Errors in procedure submit_journal_import. please delete lines in GL Interface. Errors' ||
                   SQLERRM;
      debug_message(x_message);
    
  END submit_journal_import;

  PROCEDURE output_report(errbuf     OUT VARCHAR2,
                          retcode    OUT VARCHAR2,
                          p_batch_id IN NUMBER) IS
  
    l_header  VARCHAR2(10000);
    l_message VARCHAR2(32767);
  
    CURSOR l_data_csr IS
      SELECT t.request_id,
             t.source,
             t.period,
             sob.name                  sob,
             t.me_code,
             t.le_code,
             t.account,
             t.currency_code,
             gcc.concatenated_segments segments,
             t.rate_type,
             t.rate,
             t.status,
             t.message                 error_message
        FROM xxrfp.gerfp_gl_reval_staging t,
             gl_sets_of_books             sob,
             gl_code_combinations_kfv     gcc
       WHERE t.batch_id = p_batch_id
         AND t.status = g_record_status_error
         AND t.sob_id = sob.set_of_books_id(+)
         AND t.ccid = gcc.code_combination_id(+);
  
  BEGIN
    l_header := '<html><head><meta http-equiv="Content-Type" content="text/html; charset=GBK"></head>' ||
                '<body><H1>GL Revaluation Error Report</H1><BR>
                Batch Id:' || p_batch_id ||
                ' <BR>
                <table border=1 cellspacing=0 bordercolor=black frame=box>
                <tr>
                            <th nowrap>Request ID</th>
                            <th nowrap>Source</th>
                            <th nowrap>Period</th>
                            <th nowrap>SOB</th>
                            <th nowrap>ME</th>
                            <th nowrap>LE</th>
                            <th nowrap>Account</th>
                            <th nowrap>Currency</th>
                            <th nowrap>Segments</th>
                            <th nowrap>Rate Type</th>
                            <th nowrap>Rate</th>
                            <th nowrap>Status</th>
                            <th nowrap>Error Message</th>
                            </tr>';
  
    fnd_file.put_line(fnd_file.output,
                      l_header);
  
    FOR l_data_rec IN l_data_csr
    LOOP
    
      gerfp_utl.output('<tr>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.request_id) ||
                       '</td>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.source) ||
                       '</td>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.period) ||
                       '</td>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.sob) ||
                       '</td>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.me_code) ||
                       '</td>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.le_code) ||
                       '</td>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.account) ||
                       '</td>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.currency_code) ||
                       '</td>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.segments) ||
                       '</td>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.rate_type) ||
                       '</td>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.rate) ||
                       '</td>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.status) ||
                       '</td>');
      gerfp_utl.output('<td nowrap>' ||
                       gerfp_utl.convert_value4web(l_data_rec.error_message) ||
                       '</td>');
    
      gerfp_utl.output('</tr>');
    
    END LOOP;
  
    fnd_file.put_line(fnd_file.output,
                      '</table></html>');
  
  EXCEPTION
    WHEN OTHERS THEN
      l_message := 'Error in procedure output_report. Errors' || SQLERRM;
      debug_message(l_message);
      retcode := 2;
      errbuf  := l_message;
    
  END;

  PROCEDURE reval(errbuf       OUT VARCHAR2,
                  retcode      OUT VARCHAR2,
                  p_batch_id   IN NUMBER,
                  p_me_code    IN VARCHAR2,
                  p_le_code    IN VARCHAR2,
                  p_book_type  IN VARCHAR2,
                  p_mrc_type   IN VARCHAR2,
                  p_period     IN VARCHAR2,
                  p_rate_date  IN VARCHAR2,
                  p_reval_type IN VARCHAR2) IS
    l_message_p VARCHAR2(32767);
    l_status    VARCHAR2(100);
    l_group_id  NUMBER;
    l_message   VARCHAR2(32767);
  
  BEGIN
  
    debug_message('--------------Parameter List---------------
  p_batch_id:' || p_batch_id || '
  p_me_code:' || p_me_code || '
  p_le_code:' || p_le_code || '
  p_book_type:' || p_book_type || '
  p_mrc_type:' || p_mrc_type || '
  p_period:' || p_period || '
  p_rate_date:' || p_rate_date || '
  p_reval_type:' || p_reval_type);
  
    init_val_ledger(p_batch_id   => p_batch_id,
                    p_me_code    => p_me_code,
                    p_le_code    => p_le_code,
                    p_book_type  => p_book_type,
                    p_mrc_type   => p_mrc_type,
                    p_period     => p_period,
                    p_rate_date  => p_rate_date,
                    p_reval_type => p_reval_type,
                    x_message    => l_message_p,
                    x_status     => l_status);
    IF l_status = g_ret_sts_error THEN
      RAISE g_error;
    ELSIF l_status = g_ret_sts_warning THEN
      l_message := l_message || chr(10) ||
                   'Warning in procedure init_val_ledger. Errors:' ||
                   l_message_p;
      RAISE g_warning;
    END IF;
  
    cal_standard(x_message => l_message_p,
                 x_status  => l_status);
  
    IF l_status = g_ret_sts_error THEN
      RAISE g_error;
    ELSIF l_status = g_ret_sts_warning THEN
      --record errors and proceed
      l_message := l_message || chr(10) ||
                   'Warning in procedure cal_standard. Errors:' ||
                   l_message_p;
      debug_message(l_message);
      retcode := 1;
    END IF;
  
    cal_777(x_message => l_message_p,
            x_status  => l_status);
  
    IF l_status = g_ret_sts_error THEN
      RAISE g_error;
    ELSIF l_status = g_ret_sts_warning THEN
      --record errors and proceed
      l_message := l_message || chr(10) ||
                   'Warning in procedure cal_777. Errors:' || l_message_p;
      debug_message(l_message_p);
      retcode := 1;
    END IF;
  
    /*    cal_unrealized_gl(p_batch_id  => p_batch_id,
                      p_sob_id    => g_sob_id,
                      p_me_code   => p_me_code,
                      p_le_code   => p_le_code,
                      p_book_type => p_book_type,
                      p_period    => p_period,
                      x_message   => l_message,
                      x_status    => l_status);
    
    IF l_status = 'E' THEN
      RAISE g_error;
    ELSIF l_status = 'W' THEN
      l_message := 'Warning in procedure cal_unrealized_gl. Errors:' ||
                   l_message;
      debug_message(l_message);
      retcode := 1;
      errbuf  := l_message;
    END IF;*/
  
    cal_fx(x_message => l_message_p,
           x_status  => l_status);
  
    IF l_status = g_ret_sts_error THEN
      RAISE g_error;
    ELSIF l_status = g_ret_sts_warning THEN
      l_message := l_message || chr(10) ||
                   'Warning in procedure cal_fx. Errors:' || l_message_p;
      RAISE g_warning;
    END IF;
  
    insert_gl_interface(x_group_id => l_group_id,
                        x_message  => l_message_p,
                        x_status   => l_status);
  
    IF l_status = g_ret_sts_error THEN
      RAISE g_error;
    ELSIF l_status = g_ret_sts_warning THEN
      l_message := l_message || chr(10) ||
                   'Warning in procedure insert_gl_interface. Errors:' ||
                   l_message_p;
      RAISE g_warning;
    END IF;
    ---transation finished here
    COMMIT;
  
    submit_journal_import(p_sob_id      => g_sob_id,
                          p_group_id    => l_group_id,
                          p_user_source => g_reval_type,
                          x_message     => l_message_p,
                          x_status      => l_status);
  
    IF l_status = g_ret_sts_error THEN
      l_message := l_message || chr(10) ||
                   'Error in procedure submit_journal_import. Errors:' ||
                   l_message_p;
      RAISE g_warning;
    ELSIF l_status = g_ret_sts_warning THEN
      l_message := l_message || chr(10) ||
                   'Warning in procedure submit_journal_import. Errors:' ||
                   l_message_p;
      RAISE g_warning;
    END IF;
  
  EXCEPTION
    WHEN g_warning THEN
    
      l_message := l_message || chr(10) ||
                   'Warning in procedure reval. Errors:' || l_message_p;
      debug_message(l_message);
      retcode := 1;
      -- errbuf  := l_message;
    WHEN g_error THEN
    
      l_message := l_message || chr(10) ||
                   'Errors in procedure reval. Errors:' || l_message_p;
      debug_message(l_message);
      retcode := 2;
      -- errbuf  := l_message;
    WHEN OTHERS THEN
    
      l_message := l_message || chr(10) ||
                   'Errors in procedure reval. Errors:' || SQLERRM;
      debug_message(l_message);
      retcode := 2;
      -- errbuf  := l_message;
  END;

  PROCEDURE remeasure_by_user(errbuf      OUT VARCHAR2,
                              retcode     OUT VARCHAR2,
                              p_me_code   IN VARCHAR2,
                              p_le_code   IN VARCHAR2,
                              p_book_type IN VARCHAR2,
                              p_sob_id    IN NUMBER,
                              p_period    IN VARCHAR2,
                              p_rate_date IN VARCHAR2) IS
  
    l_message  VARCHAR2(32767);
    l_batch_id NUMBER;
    l_p_sod_id NUMBER;
    l_mrc_type VARCHAR2(32767);
    l_req_id   NUMBER;
  
    l_phase         VARCHAR2(2000);
    l_status        VARCHAR2(2000);
    l_dev_phase     VARCHAR2(2000);
    l_dev_status    VARCHAR2(2000);
    l_return_status BOOLEAN;
    l_msg_data      VARCHAR2(2000);
  
    CURSOR l_data_csr IS
      SELECT bus.*
        FROM xxrfp_shelton_bus_map bus,
             gl_sets_of_books      sob
       WHERE bus.primary_flag = 'Y'
         AND bus.le_code != '000000'
         AND bus.me_code = nvl(p_me_code,
                               bus.me_code)
         AND bus.le_code = nvl(p_le_code,
                               bus.le_code)
         AND bus.book_type = nvl(p_book_type,
                                 bus.book_type)
         AND bus.enable_flag = 'Y'
         AND bus.fx_enable_flag = 'Y'
         AND bus.sob_name = sob.name
         AND sob.set_of_books_id = l_p_sod_id;
  
  BEGIN
  
    debug_message('--------------Parameter List---------------
  p_me_code:' || p_me_code || '
  p_le_code:' || p_le_code || '
  p_book_type:' || p_book_type || '
  p_sob_id:' || p_sob_id || '
  p_period:' || p_period || '
  p_rate_date:' || p_rate_date);
  
    SELECT gerfp_gl_reval_batch_id_s.nextval INTO l_batch_id FROM dual;
  
    SELECT sob.mrc_sob_type_code
      INTO l_mrc_type
      FROM gl_sets_of_books sob
     WHERE sob.set_of_books_id = p_sob_id;
  
    debug_message('mrc_type:' || l_mrc_type);
  
    IF l_mrc_type = 'N' THEN
      l_p_sod_id := p_sob_id;
      l_mrc_type := 'P';
    ELSE
    
      SELECT ba.primary_set_of_books_id
        INTO l_p_sod_id
        FROM gl_mc_book_assignments ba
       WHERE decode(l_mrc_type,
                    'P',
                    ba.primary_set_of_books_id,
                    ba.reporting_set_of_books_id) = p_sob_id;
    END IF;
  
    debug_message('primary sod_id:' || l_p_sod_id);
  
    --submit reval program
  
    FOR l_data_rec IN l_data_csr
    LOOP
    
      IF l_data_rec.export4 = 'LC'
         AND l_mrc_type = 'R' THEN
      
        l_message := 'Remeasurement cannot be run in Reporting Book for Local Currency ledger.';
        RAISE g_error;
      
      END IF;
    
      l_req_id := fnd_request.submit_request(application => 'XXRFP',
                                             program     => 'GERFPGR',
                                             description => NULL,
                                             start_time  => SYSDATE,
                                             sub_request => FALSE,
                                             argument1   => to_char(l_batch_id),
                                             argument2   => l_data_rec.me_code,
                                             argument3   => l_data_rec.le_code,
                                             argument4   => l_data_rec.book_type,
                                             argument5   => l_mrc_type,
                                             argument6   => p_period,
                                             argument7   => p_rate_date,
                                             argument8   => c_source_remeasure);
    
      COMMIT;
    
      debug_message('Submit Revaluation Request id :' || l_req_id);
    
      IF l_req_id = 0 THEN
        l_message := 'Failed to submit Revaluation. Errors:' ||
                     fnd_message.get;
        RAISE g_error;
      END IF;
    
    END LOOP;
  
    IF nvl(l_req_id,
           0) = 0 THEN
    
      l_message := 'No Requests raised';
      RAISE g_warning;
    END IF;
  
    l_return_status := fnd_concurrent.wait_for_request(request_id => l_req_id,
                                                       INTERVAL   => 5,
                                                       max_wait   => 0,
                                                       phase      => l_phase,
                                                       status     => l_status,
                                                       dev_phase  => l_dev_phase,
                                                       dev_status => l_dev_status,
                                                       message    => l_msg_data);
  
    IF l_return_status = TRUE THEN
    
      IF l_phase != 'Completed'
         OR l_status IN ('Cancelled',
                         'Error',
                         'Terminated') THEN
      
        debug_message('Request failed. Status:' || l_dev_status ||
                      ' Phase:' || l_dev_phase);
        RAISE g_warning;
      
      END IF;
    
    ELSE
    
      debug_message('WAIT FOR REQUEST FAILED - STATUS UNKNOWN');
      RAISE g_warning;
    
    END IF;
  
    --submit report program  
    l_req_id := fnd_request.submit_request(application => 'XXRFP',
                                           program     => 'GERFPGRSR',
                                           description => NULL,
                                           start_time  => SYSDATE,
                                           sub_request => FALSE,
                                           argument1   => to_char(l_batch_id));
  
    COMMIT;
  
    debug_message('Submit Revaluation Status Report Request id :' ||
                  l_req_id);
  
    IF l_req_id = 0 THEN
      l_message := 'Failed to submit Revaluation Status Report . Errors:' ||
                   fnd_message.get;
      RAISE g_warning;
    END IF;
  
  EXCEPTION
    WHEN g_warning THEN
    
      l_message := 'Warning in procedure remeasure_by_user. Errors:' ||
                   l_message;
      debug_message(l_message);
      retcode := 1;
      errbuf  := l_message;
    WHEN g_error THEN
    
      l_message := 'Errors in procedure remeasure_by_user. Errors:' ||
                   l_message;
      debug_message(l_message);
      retcode := 2;
      errbuf  := l_message;
    WHEN OTHERS THEN
    
      l_message := 'Errors in procedure remeasure_by_user. Errors:' ||
                   SQLERRM;
      debug_message(l_message);
      retcode := 2;
      errbuf  := l_message;
  END;

  PROCEDURE translate_by_user(errbuf      OUT VARCHAR2,
                              retcode     OUT VARCHAR2,
                              p_me_code   IN VARCHAR2,
                              p_le_code   IN VARCHAR2,
                              p_book_type IN VARCHAR2,
                              p_sob_id    IN NUMBER,
                              p_period    IN VARCHAR2,
                              p_rate_date IN VARCHAR2) IS
    l_message  VARCHAR2(32767);
    l_batch_id NUMBER;
    l_p_sod_id NUMBER;
    l_mrc_type VARCHAR2(32767);
    l_req_id   NUMBER;
  
    l_phase         VARCHAR2(2000); -- phase
    l_status        VARCHAR2(2000); -- status
    l_dev_phase     VARCHAR2(2000); -- dev phase
    l_dev_status    VARCHAR2(2000); -- dev status
    l_return_status BOOLEAN; -- return status
    l_msg_data      VARCHAR2(2000);
  
    CURSOR l_data_csr IS
      SELECT bus.*
        FROM xxrfp_shelton_bus_map bus,
             gl_sets_of_books      sob
       WHERE bus.primary_flag = 'Y'
         AND bus.le_code != '000000'
         AND bus.me_code = nvl(p_me_code,
                               bus.me_code)
         AND bus.le_code = nvl(p_le_code,
                               bus.le_code)
         AND bus.book_type = nvl(p_book_type,
                                 bus.book_type)
         AND bus.enable_flag = 'Y'
         AND bus.fx_enable_flag = 'Y'
         AND bus.sob_name = sob.name
         AND sob.set_of_books_id = l_p_sod_id;
  
  BEGIN
  
    debug_message('--------------Parameter List---------------
  p_me_code:' || p_me_code || '
  p_le_code:' || p_le_code || '
  p_book_type:' || p_book_type || '
  p_sob_id:' || p_sob_id || '
  p_period:' || p_period || '
  p_rate_date:' || p_rate_date);
  
    SELECT gerfp_gl_reval_batch_id_s.nextval INTO l_batch_id FROM dual;
  
    SELECT sob.mrc_sob_type_code
      INTO l_mrc_type
      FROM gl_sets_of_books sob
     WHERE sob.set_of_books_id = p_sob_id;
  
    debug_message('mrc_type:' || l_mrc_type);
  
    IF l_mrc_type = 'N' THEN
      l_p_sod_id := p_sob_id;
      l_mrc_type := 'P';
    ELSE
    
      SELECT ba.primary_set_of_books_id
        INTO l_p_sod_id
        FROM gl_mc_book_assignments ba
       WHERE decode(l_mrc_type,
                    'P',
                    ba.primary_set_of_books_id,
                    ba.reporting_set_of_books_id) = p_sob_id;
    END IF;
  
    --submit reval program
  
    FOR l_data_rec IN l_data_csr
    LOOP
    
      IF NOT (l_data_rec.export4 = 'LC' AND l_mrc_type = 'R') THEN
      
        l_message := 'Translation only can be run in Reporting Book for Local Currency Ledger. mrc_sob_type_code:' ||
                     l_mrc_type || '; Functional Currency:' ||
                     l_data_rec.export4;
        RAISE g_error;
      
      END IF;
    
      l_req_id := fnd_request.submit_request(application => 'XXRFP',
                                             program     => 'GERFPGR',
                                             description => NULL,
                                             start_time  => SYSDATE,
                                             sub_request => FALSE,
                                             argument1   => to_char(l_batch_id),
                                             argument2   => l_data_rec.me_code,
                                             argument3   => l_data_rec.le_code,
                                             argument4   => l_data_rec.book_type,
                                             argument5   => l_mrc_type,
                                             argument6   => p_period,
                                             argument7   => p_rate_date,
                                             argument8   => c_source_translate);
    
      COMMIT;
    
      debug_message('Submit Revaluation Request id :' || l_req_id);
    
      IF l_req_id = 0 THEN
        l_message := 'Failed to submit Revaluation. Errors:' ||
                     fnd_message.get;
        RAISE g_error;
      END IF;
    
    END LOOP;
  
    IF nvl(l_req_id,
           0) = 0 THEN
    
      l_message := 'No Requests raised';
      RAISE g_warning;
    END IF;
  
    l_return_status := fnd_concurrent.wait_for_request(request_id => l_req_id,
                                                       INTERVAL   => 5,
                                                       max_wait   => 0,
                                                       phase      => l_phase,
                                                       status     => l_status,
                                                       dev_phase  => l_dev_phase,
                                                       dev_status => l_dev_status,
                                                       message    => l_msg_data);
  
    IF l_return_status = TRUE THEN
    
      IF l_phase != 'Completed'
         OR l_status IN ('Cancelled',
                         'Error',
                         'Terminated') THEN
      
        debug_message('Request failed. Status:' || l_dev_status ||
                      ' Phase:' || l_dev_phase);
        RAISE g_warning;
      
      END IF;
    
    ELSE
    
      debug_message('WAIT FOR REQUEST FAILED - STATUS UNKNOWN');
      RAISE g_warning;
    
    END IF;
  
    --submit report program  
    l_req_id := fnd_request.submit_request(application => 'XXRFP',
                                           program     => 'GERFPGRSR',
                                           description => NULL,
                                           start_time  => SYSDATE,
                                           sub_request => FALSE,
                                           argument1   => to_char(l_batch_id));
  
    COMMIT;
  
    debug_message('Submit Revaluation Status Report Request id :' ||
                  l_req_id);
  
    IF l_req_id = 0 THEN
      l_message := 'Failed to submit Revaluation Status Report . Errors:' ||
                   fnd_message.get;
      RAISE g_warning;
    END IF;
  
  EXCEPTION
    WHEN g_warning THEN
    
      l_message := 'Warning in procedure revalue_by_user. Errors:' ||
                   l_message;
      debug_message(l_message);
      retcode := 1;
      errbuf  := l_message;
    WHEN g_error THEN
    
      l_message := 'Errors in procedure revalue_by_user. Errors:' ||
                   l_message;
      debug_message(l_message);
      retcode := 2;
      errbuf  := l_message;
    WHEN OTHERS THEN
    
      l_message := 'Errors in procedure revalue_by_user. Errors:' ||
                   SQLERRM;
      debug_message(l_message);
      retcode := 2;
      errbuf  := l_message;
  END;

  PROCEDURE remeasure(errbuf      OUT VARCHAR2,
                      retcode     OUT VARCHAR2,
                      p_me_code   IN VARCHAR2,
                      p_le_code   IN VARCHAR2,
                      p_book_type IN VARCHAR2,
                      p_mrc_type  IN VARCHAR2,
                      p_sob_id    IN NUMBER,
                      p_period    IN VARCHAR2,
                      p_rate_date IN VARCHAR2) IS
  
    l_message  VARCHAR2(32767);
    l_batch_id NUMBER;
    l_req_id   NUMBER;
  
    l_phase         VARCHAR2(2000);
    l_status        VARCHAR2(2000);
    l_dev_phase     VARCHAR2(2000);
    l_dev_status    VARCHAR2(2000);
    l_return_status BOOLEAN;
    l_msg_data      VARCHAR2(2000);
  
    l_idx NUMBER;
  
    TYPE l_request_ids_t IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    l_request_ids l_request_ids_t;
  
    CURSOR l_data_csr IS
      SELECT bus.*
        FROM xxrfp_shelton_bus_map bus,
             gl_sets_of_books      sob
       WHERE bus.primary_flag = 'Y'
         AND bus.le_code != '000000'
         AND bus.me_code = nvl(p_me_code,
                               bus.me_code)
         AND bus.le_code = nvl(p_le_code,
                               bus.le_code)
         AND bus.book_type = nvl(p_book_type,
                                 bus.book_type)
         AND bus.enable_flag = 'Y'
         AND bus.fx_enable_flag = 'Y'
         AND bus.sob_name = sob.name
         AND sob.set_of_books_id =
             nvl(p_sob_id,
                 sob.set_of_books_id)
         AND ((p_mrc_type = 'P' AND
             bus.export4 IN ('LC',
                               'USD')) OR
             (p_mrc_type = 'R' AND bus.export4 = 'USD'));
  
  BEGIN
  
    debug_message('--------------Parameter List---------------
  p_me_code:' || p_me_code || '
  p_le_code:' || p_le_code || '
  p_book_type:' || p_book_type || '
  p_mrc_type:' || p_mrc_type || '
  p_sob_id:' || p_sob_id || '
  p_period:' || p_period || '
  p_rate_date:' || p_rate_date);
  
    SELECT gerfp_gl_reval_batch_id_s.nextval INTO l_batch_id FROM dual;
  
    --submit reval program
  
    FOR l_data_rec IN l_data_csr
    LOOP
    
      debug_message('start process ledger: ' || l_data_rec.me_code || '.' ||
                    l_data_rec.le_code);
    
      debug_message('functional currency: ' || l_data_rec.export4);
    
      -- functional currency
      IF ((p_mrc_type = 'P' AND
         l_data_rec.export4 IN ('LC',
                                  'USD')) OR
         (p_mrc_type = 'R' AND l_data_rec.export4 = 'USD')) THEN
      
        l_req_id := fnd_request.submit_request(application => 'XXRFP',
                                               program     => 'GERFPGR',
                                               description => NULL,
                                               start_time  => SYSDATE,
                                               sub_request => FALSE,
                                               argument1   => to_char(l_batch_id),
                                               argument2   => l_data_rec.me_code,
                                               argument3   => l_data_rec.le_code,
                                               argument4   => l_data_rec.book_type,
                                               argument5   => p_mrc_type,
                                               argument6   => p_period,
                                               argument7   => p_rate_date,
                                               argument8   => c_source_remeasure);
      
        COMMIT;
      
        debug_message('Submit Remeasurement in ' || p_mrc_type ||
                      ' Book. Request id :' || l_req_id);
      
        IF l_req_id = 0 THEN
          debug_message('Failed to submit Request. Errors:' ||
                        fnd_message.get);
          retcode := 1;
        ELSE
        
          l_request_ids(nvl(l_request_ids.last, 0) + 1) := l_req_id;
        
        END IF;
      
      END IF;
    
    END LOOP;
  
    IF nvl(l_request_ids.count,
           0) = 0 THEN
    
      l_message := 'No data to process';
      RAISE g_warning;
    END IF;
  
    l_idx := l_request_ids.first;
    <<requests_loop>>
    WHILE l_idx IS NOT NULL
    LOOP
    
      l_return_status := fnd_concurrent.wait_for_request(request_id => l_request_ids(l_idx),
                                                         INTERVAL   => 1,
                                                         max_wait   => 0,
                                                         phase      => l_phase,
                                                         status     => l_status,
                                                         dev_phase  => l_dev_phase,
                                                         dev_status => l_dev_status,
                                                         message    => l_msg_data);
    
      IF l_return_status = TRUE THEN
      
        debug_message('Request:' || l_request_ids(l_idx) || '|' || l_phase || '|' ||
                      l_status || '|' || l_msg_data);
      
        IF l_phase != 'Completed'
           OR l_status IN ('Cancelled',
                           'Error',
                           'Terminated') THEN
        
          retcode := 1;
        
        END IF;
      
      ELSE
      
        debug_message('WAIT FOR REQUEST:' || l_request_ids(l_idx) ||
                      ' FAILED - STATUS UNKNOWN');
      
      END IF;
    
      l_idx := l_request_ids.next(l_idx);
    END LOOP requests_loop;
  
    --submit report program  
    l_req_id := fnd_request.submit_request(application => 'XXRFP',
                                           program     => 'GERFPGRSR',
                                           description => NULL,
                                           start_time  => SYSDATE,
                                           sub_request => FALSE,
                                           argument1   => to_char(l_batch_id));
  
    COMMIT;
  
    debug_message('Submit Revaluation Status Report Request id :' ||
                  l_req_id);
  
    IF l_req_id = 0 THEN
      l_message := 'Failed to submit Revaluation Status Report . Errors:' ||
                   fnd_message.get;
      RAISE g_warning;
    END IF;
  
  EXCEPTION
    WHEN g_warning THEN
    
      l_message := 'Warning in procedure remeasure. Errors:' || l_message;
      debug_message(l_message);
      retcode := 1;
      errbuf  := l_message;
    WHEN g_error THEN
    
      l_message := 'Errors in procedure remeasure. Errors:' || l_message;
      debug_message(l_message);
      retcode := 2;
      errbuf  := l_message;
    WHEN OTHERS THEN
    
      l_message := 'Errors in procedure remeasure. Errors:' || SQLERRM;
      debug_message(l_message);
      retcode := 2;
      errbuf  := l_message;
  END;

  PROCEDURE translate(errbuf      OUT VARCHAR2,
                      retcode     OUT VARCHAR2,
                      p_me_code   IN VARCHAR2,
                      p_le_code   IN VARCHAR2,
                      p_book_type IN VARCHAR2,
                      p_sob_id    IN NUMBER,
                      p_period    IN VARCHAR2,
                      p_rate_date IN VARCHAR2) IS
  
    l_message  VARCHAR2(32767);
    l_batch_id NUMBER;
    l_req_id   NUMBER;
  
    l_phase         VARCHAR2(2000);
    l_status        VARCHAR2(2000);
    l_dev_phase     VARCHAR2(2000);
    l_dev_status    VARCHAR2(2000);
    l_return_status BOOLEAN;
    l_msg_data      VARCHAR2(2000);
  
    l_idx NUMBER;
  
    TYPE l_request_ids_t IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    l_request_ids l_request_ids_t;
  
    CURSOR l_data_csr IS
      SELECT bus.*
        FROM xxrfp_shelton_bus_map bus,
             gl_sets_of_books      sob
       WHERE bus.primary_flag = 'Y'
         AND bus.le_code != '000000'
         AND bus.me_code = nvl(p_me_code,
                               bus.me_code)
         AND bus.le_code = nvl(p_le_code,
                               bus.le_code)
         AND bus.book_type = nvl(p_book_type,
                                 bus.book_type)
         AND bus.enable_flag = 'Y'
         AND bus.fx_enable_flag = 'Y'
         AND bus.sob_name = sob.name
         AND sob.set_of_books_id =
             nvl(p_sob_id,
                 sob.set_of_books_id)
         AND bus.export4 IN ('LC');
  
  BEGIN
  
    debug_message('--------------Parameter List---------------
  p_me_code:' || p_me_code || '
  p_le_code:' || p_le_code || '
  p_book_type:' || p_book_type || '
  p_sob_id:' || p_sob_id || '
  p_period:' || p_period || '
  p_rate_date:' || p_rate_date);
  
    SELECT gerfp_gl_reval_batch_id_s.nextval INTO l_batch_id FROM dual;
  
    --submit reval program
  
    FOR l_data_rec IN l_data_csr
    LOOP
    
      debug_message('start process ledger: ' || l_data_rec.me_code || '.' ||
                    l_data_rec.le_code);
    
      debug_message('functional currency: ' || l_data_rec.export4);
    
      -- functional currency
      IF l_data_rec.export4 = 'LC' THEN
      
        l_req_id := fnd_request.submit_request(application => 'XXRFP',
                                               program     => 'GERFPGR',
                                               description => NULL,
                                               start_time  => SYSDATE,
                                               sub_request => FALSE,
                                               argument1   => to_char(l_batch_id),
                                               argument2   => l_data_rec.me_code,
                                               argument3   => l_data_rec.le_code,
                                               argument4   => l_data_rec.book_type,
                                               argument5   => 'R',
                                               argument6   => p_period,
                                               argument7   => p_rate_date,
                                               argument8   => c_source_translate);
      
        COMMIT;
      
        debug_message('Submit Translate in R Book. Request id :' ||
                      l_req_id);
      
        IF l_req_id = 0 THEN
          debug_message('Failed to submit Request. Errors:' ||
                        fnd_message.get);
          retcode := 1;
        ELSE
        
          l_request_ids(nvl(l_request_ids.last, 0) + 1) := l_req_id;
        
        END IF;
      
        -- functional currency
      END IF;
    
    END LOOP;
  
    IF nvl(l_request_ids.count,
           0) = 0 THEN
    
      l_message := 'No data to process';
      RAISE g_warning;
    END IF;
  
    l_idx := l_request_ids.first;
    <<requests_loop>>
    WHILE l_idx IS NOT NULL
    LOOP
    
      l_return_status := fnd_concurrent.wait_for_request(request_id => l_request_ids(l_idx),
                                                         INTERVAL   => 1,
                                                         max_wait   => 0,
                                                         phase      => l_phase,
                                                         status     => l_status,
                                                         dev_phase  => l_dev_phase,
                                                         dev_status => l_dev_status,
                                                         message    => l_msg_data);
    
      IF l_return_status = TRUE THEN
      
        debug_message('Request:' || l_request_ids(l_idx) || '|' || l_phase || '|' ||
                      l_status || '|' || l_msg_data);
      
        IF l_phase != 'Completed'
           OR l_status IN ('Cancelled',
                           'Error',
                           'Terminated') THEN
        
          retcode := 1;
        
        END IF;
      
      ELSE
      
        debug_message('WAIT FOR REQUEST:' || l_request_ids(l_idx) ||
                      ' FAILED - STATUS UNKNOWN');
      
      END IF;
    
      l_idx := l_request_ids.next(l_idx);
    END LOOP requests_loop;
  
    --submit report program  
    l_req_id := fnd_request.submit_request(application => 'XXRFP',
                                           program     => 'GERFPGRSR',
                                           description => NULL,
                                           start_time  => SYSDATE,
                                           sub_request => FALSE,
                                           argument1   => to_char(l_batch_id));
  
    COMMIT;
  
    debug_message('Submit Revaluation Status Report Request id :' ||
                  l_req_id);
  
    IF l_req_id = 0 THEN
      l_message := 'Failed to submit Revaluation Status Report . Errors:' ||
                   fnd_message.get;
      RAISE g_warning;
    END IF;
  
  EXCEPTION
    WHEN g_warning THEN
    
      l_message := 'Warning in procedure translate. Errors:' || l_message;
      debug_message(l_message);
      retcode := 1;
      errbuf  := l_message;
    WHEN g_error THEN
    
      l_message := 'Errors in procedure translate. Errors:' || l_message;
      debug_message(l_message);
      retcode := 2;
      errbuf  := l_message;
    WHEN OTHERS THEN
    
      l_message := 'Errors in procedure translate. Errors:' || SQLERRM;
      debug_message(l_message);
      retcode := 2;
      errbuf  := l_message;
  END;

END gerfp_gl_reval_pub;
/
