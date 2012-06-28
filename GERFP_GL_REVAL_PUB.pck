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

  -- error message
  -- performance

  -- Author  : HW70001208
  -- Created : 6/3/2012 9:54:31 PM
  -- Purpose : 
  g_pkg_name CONSTANT VARCHAR2(30) := upper('gerfp_gl_reval_pub');

  c_debug_switch CONSTANT VARCHAR2(1) := 'Y';

  g_skip_record_exception EXCEPTION;

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
    
      hw_log.info(p_message);
    
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END debug_message;

  FUNCTION request_lock(p_me_code   IN VARCHAR2,
                        p_le_code   IN VARCHAR2,
                        p_book_type IN VARCHAR2,
                        p_mrc_type  IN VARCHAR2) RETURN NUMBER IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  
    l_lock_hanlder VARCHAR2(32767);
    l_lock_result  NUMBER;
  BEGIN
    dbms_lock.allocate_unique('gerfp_gl_reval_pub:' || p_me_code || '.' ||
                              p_le_code || '.' || p_book_type || '.' ||
                              p_mrc_type,
                              l_lock_hanlder);
    l_lock_result := dbms_lock.request(lockhandle        => l_lock_hanlder,
                                       lockmode          => dbms_lock.x_mode,
                                       timeout           => 0,
                                       release_on_commit => FALSE);
    RETURN l_lock_result;
  END;

  FUNCTION get_rate(p_rate_date          IN DATE,
                    p_rate_type          IN VARCHAR2,
                    p_from_currency_code IN VARCHAR2,
                    p_to_currency_code   IN VARCHAR2) RETURN NUMBER IS
    l_rate NUMBER;
    l_api_name CONSTANT VARCHAR2(30) := 'get_rate';
  BEGIN
  
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
    
      hw_api.stack_message('Rate not defined. ' || p_rate_type ||
                           ' rate from Currency:' || p_from_currency_code ||
                           ' to ' || p_to_currency_code || ' on ' ||
                           p_rate_date);
      RETURN 0;
    
    WHEN OTHERS THEN
      IF fnd_msg_pub.check_msg_level(fnd_msg_pub.g_msg_lvl_unexp_error) THEN
        fnd_msg_pub.add_exc_msg(g_pkg_name,
                                l_api_name);
      END IF;
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

  PROCEDURE update_process_status(p_process_status IN VARCHAR2,
                                  p_message        IN VARCHAR2) IS
  BEGIN
    UPDATE gerfp_gl_reval_staging t
       SET t.status  = p_process_status,
           t.message = substr(p_message,
                              1,
                              4000)
     WHERE t.batch_id = g_batch_id
       AND t.me_code = g_me_code
       AND t.le_code = g_le_code
       AND t.book_type = g_book_type
       AND t.status = g_record_status_ready
       AND t.sob_id = g_sob_id;
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
  PROCEDURE init_val_ledger(p_init_msg_list IN VARCHAR2 DEFAULT fnd_api.g_false,
                            p_commit        IN VARCHAR2 DEFAULT fnd_api.g_false,
                            x_return_status OUT NOCOPY VARCHAR2,
                            x_msg_count     OUT NOCOPY NUMBER,
                            x_msg_data      OUT NOCOPY VARCHAR2,
                            p_batch_id      IN NUMBER,
                            p_me_code       IN VARCHAR2,
                            p_le_code       IN VARCHAR2,
                            p_book_type     IN VARCHAR2,
                            p_mrc_type      IN VARCHAR2,
                            p_period        IN VARCHAR2,
                            p_rate_date     IN VARCHAR2,
                            p_reval_type    IN VARCHAR2) IS
  
    l_api_name       CONSTANT VARCHAR2(30) := 'init_val_ledger';
    l_savepoint_name CONSTANT VARCHAR2(30) := 'init_val_ledger';
    l_procedure_status VARCHAR2(30);
  
    l_p_sob_id NUMBER;
    l_r_sob_id NUMBER;
  
    l_fx_enable_flag      VARCHAR2(100);
    l_bus_enable_flag     VARCHAR2(100);
    l_cta_account         VARCHAR2(100);
    l_fx_account          VARCHAR2(100);
    l_fx_cc               VARCHAR2(100);
    l_fx_project          VARCHAR2(100);
    l_fx_reference        VARCHAR2(100);
    l_fx_concat_segments  VARCHAR2(1000);
    l_cta_concat_segments VARCHAR2(1000);
    l_ext_precision       NUMBER;
    l_min_acct_unit       NUMBER;
    l_closing_status      VARCHAR2(100);
    l_lock_result         NUMBER;
  
    l_period_start_date DATE;
    l_period_end_date   DATE;
  
  BEGIN
  
    x_return_status := hw_api.start_activity(p_pkg_name       => g_pkg_name,
                                             p_api_name       => l_api_name,
                                             p_savepoint_name => l_savepoint_name,
                                             p_init_msg_list  => p_init_msg_list);
  
    hw_api.raise_exception(p_return_status     => x_return_status,
                           px_procedure_status => l_procedure_status);
  
    debug_message('----------------------' ||
                  'Start initialize and validate on ledger level' ||
                  '----------------------');
  
    debug_message('request lock');
    l_lock_result := request_lock(p_me_code   => p_me_code,
                                  p_le_code   => p_le_code,
                                  p_book_type => p_book_type,
                                  p_mrc_type  => p_mrc_type);
    IF l_lock_result NOT IN (0,
                             4) THEN
      hw_api.stack_message('failed to request lock for ledger :' ||
                           p_me_code || '.' || p_le_code || '.' ||
                           p_book_type || ' IN Book-' || p_mrc_type ||
                           '. Lock result:' || l_lock_result);
      RAISE fnd_api.g_exc_unexpected_error;
    
    ELSE
      debug_message('got lock');
    
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
      hw_api.stack_message('wrong Reval Type=' || g_reval_type);
      RAISE fnd_api.g_exc_unexpected_error;
    END IF;
  
    IF p_mrc_type NOT IN ('P',
                          'R') THEN
    
      hw_api.stack_message('wrong mrc_type:' || p_mrc_type);
      RAISE fnd_api.g_exc_unexpected_error;
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
      hw_api.stack_message('failed to get SOB. ');
      RAISE fnd_api.g_exc_unexpected_error;
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
      
        hw_api.stack_message('Error in getting SOB information, error:' ||
                             SQLERRM);
        RAISE fnd_api.g_exc_unexpected_error;
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
        hw_api.stack_message('Error in getting PSOB information, error:' ||
                             SQLERRM);
        RAISE fnd_api.g_exc_unexpected_error;
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
        hw_api.stack_message('Error in getting BUS information, error:' ||
                             SQLERRM);
        RAISE fnd_api.g_exc_unexpected_error;
      
    END;
  
    IF nvl(l_fx_enable_flag,
           'N') != 'Y' THEN
    
      hw_api.stack_message('The Ledger does not enable FX program');
      RAISE fnd_api.g_exc_unexpected_error;
    
    END IF;
  
    IF nvl(l_bus_enable_flag,
           'N') != 'Y' THEN
    
      hw_api.stack_message('The Ledger is disabled');
      RAISE fnd_api.g_exc_unexpected_error;
    
    END IF;
  
    --validate local currency
  
    IF g_fc IS NULL
       OR g_fc NOT IN ('LC',
                       'USD') THEN
      hw_api.stack_message('Local Currency:' || g_fc || ' is invalid');
      RAISE fnd_api.g_exc_unexpected_error;
    END IF;
  
    --validate line of business of ME
    BEGIN
      SELECT t.me_mars_line_of_business
        INTO g_me_line_of_business
        FROM xxrfp.gerfp_vld_ccl_me_lov t
       WHERE t.ccl_me_id = p_me_code;
    
      debug_message('line of business of ME:' || g_me_line_of_business);
    
    EXCEPTION
      WHEN no_data_found THEN
        hw_api.stack_message('ME:' || p_me_code || ' not exist in CCL');
        RAISE fnd_api.g_exc_unexpected_error;
      WHEN OTHERS THEN
        hw_api.stack_message('Error in getting CCL ME , error:' || SQLERRM);
        RAISE fnd_api.g_exc_unexpected_error;
    END;
  
    IF g_me_line_of_business IS NULL
       OR
       g_me_line_of_business NOT IN ('F',
                                     'I') THEN
      hw_api.stack_message('line of business of ME:' ||
                           g_me_line_of_business || ' is invalid');
      RAISE fnd_api.g_exc_unexpected_error;
    END IF;
  
    ---validation period
    BEGIN
    
      SELECT t.closing_status,
             t.start_date,
             t.end_date
        INTO l_closing_status,
             l_period_start_date,
             l_period_end_date
        FROM gl_period_statuses t
       WHERE t.set_of_books_id = g_sob_id
         AND t.period_name = p_period
         AND t.application_id = 101;
    EXCEPTION
      WHEN OTHERS THEN
        hw_api.stack_message('Error in getting Period, error:' || SQLERRM);
        RAISE fnd_api.g_exc_unexpected_error;
    END;
  
    IF nvl(l_closing_status,
           'C') != 'O' THEN
      hw_api.stack_message('Period is not open');
      RAISE fnd_api.g_exc_unexpected_error;
    
    END IF;
  
    IF trunc(SYSDATE) BETWEEN l_period_start_date AND l_period_end_date THEN
      g_effective_date := trunc(SYSDATE);
    ELSE
      g_effective_date := l_period_end_date;
    END IF;
  
    debug_message('Effective Date:' || g_effective_date);
  
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
      
        hw_api.stack_message('Error in getting FX CCID for segments:' ||
                             l_fx_concat_segments || '. error:' ||
                             fnd_message.get);
        RAISE fnd_api.g_exc_unexpected_error;
      
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
      
        hw_api.stack_message('Error in getting CTA CCID for segments:' ||
                             l_cta_concat_segments || '. error:' ||
                             fnd_message.get);
        RAISE fnd_api.g_exc_unexpected_error;
      
      END IF;
    END IF;
  
    fnd_currency.get_info(g_sob_currency,
                          g_precision,
                          l_ext_precision,
                          l_min_acct_unit);
    g_threshold_amount := power(0.1,
                                g_precision);
    debug_message('currency precision:' || g_precision);
    debug_message('threshold_amount:' || g_threshold_amount);
  
    x_return_status := hw_api.end_activity(p_pkg_name         => g_pkg_name,
                                           p_api_name         => l_api_name,
                                           p_commit           => p_commit,
                                           p_procedure_status => l_procedure_status,
                                           x_msg_count        => x_msg_count,
                                           x_msg_data         => x_msg_data);
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_error,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    WHEN fnd_api.g_exc_unexpected_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_unexp,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    WHEN OTHERS THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_others,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    
  END;

  PROCEDURE cal_remeasure(p_init_msg_list IN VARCHAR2 DEFAULT fnd_api.g_false,
                          p_commit        IN VARCHAR2 DEFAULT fnd_api.g_false,
                          x_return_status OUT NOCOPY VARCHAR2,
                          x_msg_count     OUT NOCOPY NUMBER,
                          x_msg_data      OUT NOCOPY VARCHAR2,
                          p_ccid          IN NUMBER,
                          p_remeas_value  IN VARCHAR2,
                          x_results       OUT g_staging_table_t) IS
  
    l_api_name       CONSTANT VARCHAR2(30) := 'cal_remeasure';
    l_savepoint_name CONSTANT VARCHAR2(30) := 'cal_remeasure';
    l_procedure_status VARCHAR2(30);
  
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
  
    x_return_status := hw_api.start_activity(p_pkg_name       => g_pkg_name,
                                             p_api_name       => l_api_name,
                                             p_savepoint_name => l_savepoint_name,
                                             p_init_msg_list  => p_init_msg_list);
  
    hw_api.raise_exception(p_return_status     => x_return_status,
                           px_procedure_status => l_procedure_status);
  
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
                              g_sob_currency);
      IF nvl(l_rate,
             0) = 0 THEN
        debug_message('Errors in Getting Rate: ' ||
                      fnd_msg_pub.get(fnd_msg_pub.g_last,
                                      'F'));
        RAISE fnd_api.g_exc_unexpected_error;
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
  
    x_return_status := hw_api.end_activity(p_pkg_name         => g_pkg_name,
                                           p_api_name         => l_api_name,
                                           p_commit           => p_commit,
                                           p_procedure_status => l_procedure_status,
                                           x_msg_count        => x_msg_count,
                                           x_msg_data         => x_msg_data);
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_error,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    
    WHEN fnd_api.g_exc_unexpected_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_unexp,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    
    WHEN OTHERS THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_others,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    
  END;

  PROCEDURE cal_translate(p_init_msg_list IN VARCHAR2 DEFAULT fnd_api.g_false,
                          p_commit        IN VARCHAR2 DEFAULT fnd_api.g_false,
                          x_return_status OUT NOCOPY VARCHAR2,
                          x_msg_count     OUT NOCOPY NUMBER,
                          x_msg_data      OUT NOCOPY VARCHAR2,
                          p_ccid          IN NUMBER,
                          p_remeas_value  IN VARCHAR2,
                          x_results       OUT g_staging_table_t) IS
    l_api_name       CONSTANT VARCHAR2(30) := 'cal_translate';
    l_savepoint_name CONSTANT VARCHAR2(30) := 'cal_translate';
    l_procedure_status VARCHAR2(30);
    CURSOR l_data_crs IS
      SELECT base_view.code_combination_id,
             g_psob_currency psob_curr,
             base_view.account,
             SUM(base_view.p_accounted_ytd) p_accounted_ytd,
             SUM(base_view.r_accounted_ytd) r_accounted_ytd
        FROM (SELECT gcc.code_combination_id,
                     gcc.segment4 account,
                     0 p_accounted_ytd,
                     ((nvl(gb.begin_balance_dr,
                           0) - nvl(gb.begin_balance_cr,
                                      0)) +
                     (nvl(gb.period_net_dr,
                           0) - nvl(gb.period_net_cr,
                                      0))) r_accounted_ytd
                FROM gl_balances          gb,
                     gl_code_combinations gcc,
                     gl_sets_of_books     sob
               WHERE gcc.code_combination_id = p_ccid
                 AND gb.code_combination_id = gcc.code_combination_id
                 AND gb.currency_code = sob.currency_code
                 AND gb.set_of_books_id = sob.set_of_books_id
                 AND sob.set_of_books_id = g_sob_id
                 AND gb.period_name = g_period
              
              UNION ALL
              
              SELECT gcc.code_combination_id,
                     gcc.segment4 account,
                     ((nvl(gb2.begin_balance_dr,
                           0) - nvl(gb2.begin_balance_cr,
                                      0)) +
                     (nvl(gb2.period_net_dr,
                           0) - nvl(gb2.period_net_cr,
                                      0))) p_accounted_ytd,
                     0 r_accounted_ytd
                FROM gl_code_combinations gcc,
                     gl_sets_of_books     sob2,
                     gl_balances          gb2
               WHERE gcc.code_combination_id = p_ccid
                 AND gb2.code_combination_id = gcc.code_combination_id
                 AND gb2.currency_code = sob2.currency_code
                 AND gb2.set_of_books_id = sob2.set_of_books_id
                 AND sob2.set_of_books_id = g_psob_id
                 AND gb2.period_name = g_period) base_view
       GROUP BY base_view.code_combination_id,
                base_view.account
      HAVING nvl(SUM(base_view.p_accounted_ytd), 0) != 0 OR nvl(SUM(base_view.r_accounted_ytd), 0) != 0;
  
    l_variance_amount NUMBER;
    l_reval_amount    NUMBER;
    l_rate_type       VARCHAR2(100);
    l_rate            NUMBER;
    l_error_msg       VARCHAR2(32637);
    l_status          VARCHAR2(100);
    l_count           NUMBER;
  
  BEGIN
  
    x_return_status := hw_api.start_activity(p_pkg_name       => g_pkg_name,
                                             p_api_name       => l_api_name,
                                             p_savepoint_name => l_savepoint_name,
                                             p_init_msg_list  => p_init_msg_list);
  
    hw_api.raise_exception(p_return_status     => x_return_status,
                           px_procedure_status => l_procedure_status);
  
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
                              g_sob_currency);
      IF nvl(l_rate,
             0) = 0 THEN
        debug_message('Errors in Getting Rate: ' ||
                      fnd_msg_pub.get(fnd_msg_pub.g_last,
                                      'F'));
        RAISE fnd_api.g_exc_unexpected_error;
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
  
    x_return_status := hw_api.end_activity(p_pkg_name         => g_pkg_name,
                                           p_api_name         => l_api_name,
                                           p_commit           => p_commit,
                                           p_procedure_status => l_procedure_status,
                                           x_msg_count        => x_msg_count,
                                           x_msg_data         => x_msg_data);
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_error,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    
    WHEN fnd_api.g_exc_unexpected_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_unexp,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    
    WHEN OTHERS THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_others,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    
  END;

  PROCEDURE validate_account(p_account       IN VARCHAR2,
                             x_remeas_value  OUT VARCHAR2,
                             x_return_status OUT VARCHAR2) IS
    l_api_name CONSTANT VARCHAR2(30) := 'validate_account';
    l_line_of_business    VARCHAR2(100);
    l_account_enable_flag VARCHAR2(100);
    l_remeas_trans        VARCHAR2(100);
  
  BEGIN
  
    x_return_status := fnd_api.g_ret_sts_success;
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
      
        hw_api.stack_message('Account:' || p_account ||
                             ' not exist in CCL');
        RAISE fnd_api.g_exc_unexpected_error;
      
      WHEN OTHERS THEN
      
        hw_api.stack_message('Error in getting CCL ACCOUNT:' || p_account ||
                             'error:' || SQLERRM);
        RAISE fnd_api.g_exc_unexpected_error;
      
    END;
  
    IF l_account_enable_flag <> 'Y' THEN
      hw_api.stack_message('Account:' || p_account ||
                           ' is disabled in CCL');
      RAISE fnd_api.g_exc_unexpected_error;
    END IF;
  
    /*    IF NOT (l_line_of_business = 'ALL' OR
        g_me_line_of_business = l_line_of_business) THEN
    
      hw_api.stack_message('line_of_business is conflict on account level and ME level');
      RAISE fnd_api.g_exc_unexpected_error;
    
    END IF;*/
  
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
    
      hw_api.stack_message('remeas_value:' || x_remeas_value ||
                           ' is invalid');
      RAISE fnd_api.g_exc_unexpected_error;
    
    END IF;
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      x_return_status := fnd_api.g_ret_sts_error;
    WHEN fnd_api.g_exc_unexpected_error THEN
      x_return_status := fnd_api.g_ret_sts_unexp_error;
    WHEN OTHERS THEN
      IF fnd_msg_pub.check_msg_level(fnd_msg_pub.g_msg_lvl_unexp_error) THEN
        fnd_msg_pub.add_exc_msg(g_pkg_name,
                                l_api_name);
      END IF;
      x_return_status := fnd_api.g_ret_sts_unexp_error;
    
  END;

  PROCEDURE cal_by_ccid(p_init_msg_list IN VARCHAR2 DEFAULT fnd_api.g_false,
                        p_commit        IN VARCHAR2 DEFAULT fnd_api.g_false,
                        x_return_status OUT NOCOPY VARCHAR2,
                        x_msg_count     OUT NOCOPY NUMBER,
                        x_msg_data      OUT NOCOPY VARCHAR2,
                        p_ccid          IN NUMBER,
                        x_results       OUT g_staging_table_t) IS
  
    l_api_name       CONSTANT VARCHAR2(30) := 'cal_by_ccid';
    l_savepoint_name CONSTANT VARCHAR2(30) := 'cal_by_ccid';
    l_procedure_status VARCHAR2(30);
  
    l_remeas_value VARCHAR2(100);
    l_error_msg    VARCHAR2(32637);
  
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
  
    x_return_status := hw_api.start_activity(p_pkg_name       => g_pkg_name,
                                             p_api_name       => l_api_name,
                                             p_savepoint_name => l_savepoint_name,
                                             p_init_msg_list  => p_init_msg_list);
  
    hw_api.raise_exception(p_return_status     => x_return_status,
                           px_procedure_status => l_procedure_status);
  
    OPEN l_data_crs;
    FETCH l_data_crs
      INTO l_code_combination_id,
           l_account,
           l_concatenated_segments;
    CLOSE l_data_crs;
  
    debug_message('start processing  CCID:' || l_code_combination_id || ' ' ||
                  l_concatenated_segments);
  
    validate_account(p_account       => l_account,
                     x_remeas_value  => l_remeas_value,
                     x_return_status => x_return_status);
    IF x_return_status <> fnd_api.g_ret_sts_success THEN
    
      debug_message('Errors in Validating Account: ' ||
                    fnd_msg_pub.get(fnd_msg_pub.g_last,
                                    'F'));
      RAISE fnd_api.g_exc_unexpected_error;
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
          
            cal_remeasure(p_init_msg_list => fnd_api.g_false,
                          p_commit        => fnd_api.g_false,
                          x_return_status => x_return_status,
                          x_msg_count     => x_msg_count,
                          x_msg_data      => x_msg_data,
                          p_ccid          => l_code_combination_id,
                          p_remeas_value  => l_remeas_value,
                          x_results       => x_results);
            hw_api.raise_exception(p_return_status     => x_return_status,
                                   px_procedure_status => l_procedure_status);
          
          END IF;
        
          --Remeas Value
        ELSE
        
          l_error_msg := 'The Account is not in remeasurement scope. remeas_value: ' ||
                         l_remeas_value;
          GOTO skip_record;
        
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
          
            cal_translate(p_init_msg_list => fnd_api.g_false,
                          p_commit        => fnd_api.g_false,
                          x_return_status => x_return_status,
                          x_msg_count     => x_msg_count,
                          x_msg_data      => x_msg_data,
                          p_ccid          => l_code_combination_id,
                          p_remeas_value  => l_remeas_value,
                          x_results       => x_results);
            hw_api.raise_exception(p_return_status     => x_return_status,
                                   px_procedure_status => l_procedure_status);
          
          END IF;
        
          --Remeas Value
        ELSE
        
          l_error_msg := 'The Account is not in translating scope. remeas_value: ' ||
                         l_remeas_value;
          GOTO skip_record;
        
          --Remeas Value
        END IF;
      
        --MRC type
      ELSE
      
        hw_api.stack_message('MAC Type:' || g_mrc_type || ' is invalid');
        RAISE fnd_api.g_exc_unexpected_error;
      
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
            cal_remeasure(p_init_msg_list => fnd_api.g_false,
                          p_commit        => fnd_api.g_false,
                          x_return_status => x_return_status,
                          x_msg_count     => x_msg_count,
                          x_msg_data      => x_msg_data,
                          p_ccid          => l_code_combination_id,
                          p_remeas_value  => l_remeas_value,
                          x_results       => x_results);
            hw_api.raise_exception(p_return_status     => x_return_status,
                                   px_procedure_status => l_procedure_status);
          
          END IF;
        
          --Remeas Value
        ELSE
        
          l_error_msg := 'The Account is not in remeasurement scope. remeas_value: ' ||
                         l_remeas_value;
          GOTO skip_record;
        
          --Remeas Value
        END IF;
      
        --MRC type
      ELSIF g_mrc_type = 'R' THEN
      
        --Remeas Value
        IF l_remeas_value IN ('A',
                              'E',
                              '1') THEN
        
          IF g_reval_type = c_source_remeasure THEN
            cal_remeasure(p_init_msg_list => fnd_api.g_false,
                          p_commit        => fnd_api.g_false,
                          x_return_status => x_return_status,
                          x_msg_count     => x_msg_count,
                          x_msg_data      => x_msg_data,
                          p_ccid          => l_code_combination_id,
                          p_remeas_value  => l_remeas_value,
                          x_results       => x_results);
            hw_api.raise_exception(p_return_status     => x_return_status,
                                   px_procedure_status => l_procedure_status);
          
          END IF;
        
          --Remeas Value
        ELSE
        
          l_error_msg := 'The Account is not in translating scope. remeas_value: ' ||
                         l_remeas_value;
          GOTO skip_record;
        
          --Remeas Value
        END IF;
      
        --MRC type
      ELSE
      
        hw_api.stack_message('MAC Type:' || g_mrc_type || ' is invalid');
        RAISE fnd_api.g_exc_unexpected_error;
      
      END IF;
    
      --functional currency
    ELSE
    
      hw_api.stack_message('Local Currency:' || g_fc || ' is invalid');
      RAISE fnd_api.g_exc_unexpected_error;
    
      --functional currency
    END IF;
  
    <<skip_record>>
    IF l_error_msg IS NOT NULL THEN
      debug_message('Skip this record, As ' || l_error_msg);
    END IF;
  
    x_return_status := hw_api.end_activity(p_pkg_name         => g_pkg_name,
                                           p_api_name         => l_api_name,
                                           p_commit           => p_commit,
                                           p_procedure_status => l_procedure_status,
                                           x_msg_count        => x_msg_count,
                                           x_msg_data         => x_msg_data);
  
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_error,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
      fnd_msg_pub.delete_msg(fnd_msg_pub.count_msg);
    WHEN fnd_api.g_exc_unexpected_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_unexp,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
      fnd_msg_pub.delete_msg(fnd_msg_pub.count_msg);
    WHEN OTHERS THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_others,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
      fnd_msg_pub.delete_msg(fnd_msg_pub.count_msg);
    
  END;

  PROCEDURE cal_standard(p_init_msg_list IN VARCHAR2 DEFAULT fnd_api.g_false,
                         p_commit        IN VARCHAR2 DEFAULT fnd_api.g_false,
                         x_return_status OUT NOCOPY VARCHAR2,
                         x_msg_count     OUT NOCOPY NUMBER,
                         x_msg_data      OUT NOCOPY VARCHAR2) IS
  
    l_api_name       CONSTANT VARCHAR2(30) := 'cal_standard';
    l_savepoint_name CONSTANT VARCHAR2(30) := 'cal_standard';
    l_procedure_status VARCHAR2(30);
  
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
  
    x_return_status := hw_api.start_activity(p_pkg_name       => g_pkg_name,
                                             p_api_name       => l_api_name,
                                             p_savepoint_name => l_savepoint_name,
                                             p_init_msg_list  => p_init_msg_list);
  
    hw_api.raise_exception(p_return_status     => x_return_status,
                           px_procedure_status => l_procedure_status);
  
    debug_message('----------------------' || 'Start cal standard' ||
                  '----------------------');
  
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
        SELECT base_view.code_combination_id,
               base_view.account,
               base_view.controlled_account_type
          FROM (SELECT gcc.code_combination_id,
                       gcc.segment4 account,
                       gcc.controlled_account_type,
                       0 p_accounted_ytd,
                       ((nvl(gb.begin_balance_dr,
                             0) - nvl(gb.begin_balance_cr,
                                        0)) +
                       (nvl(gb.period_net_dr,
                             0) - nvl(gb.period_net_cr,
                                        0))) r_accounted_ytd
                  FROM gerfp_rev_gcc_v  gcc,
                       gl_balances      gb,
                       gl_sets_of_books sob
                 WHERE gcc.segment1 = g_me_code
                   AND gcc.segment2 = g_le_code
                   AND gcc.segment3 = g_book_type
                   AND gb.code_combination_id = gcc.code_combination_id
                   AND gb.currency_code = sob.currency_code
                   AND gb.set_of_books_id = sob.set_of_books_id
                   AND sob.set_of_books_id = g_sob_id
                   AND gb.period_name = g_period
                   AND gcc.remeas_value IN ('1',
                                            'A',
                                            'C')
                
                UNION ALL
                
                SELECT gcc.code_combination_id,
                       gcc.segment4 account,
                       gcc.controlled_account_type,
                       ((nvl(gb2.begin_balance_dr,
                             0) - nvl(gb2.begin_balance_cr,
                                        0)) +
                       (nvl(gb2.period_net_dr,
                             0) - nvl(gb2.period_net_cr,
                                        0))) p_accounted_ytd,
                       0 r_accounted_ytd
                  FROM gerfp_rev_gcc_v  gcc,
                       gl_sets_of_books sob2,
                       gl_balances      gb2
                 WHERE gcc.segment1 = g_me_code
                   AND gcc.segment2 = g_le_code
                   AND gcc.segment3 = g_book_type
                   AND gb2.code_combination_id = gcc.code_combination_id
                   AND gb2.currency_code = sob2.currency_code
                   AND gb2.set_of_books_id = sob2.set_of_books_id
                   AND sob2.set_of_books_id = g_psob_id
                   AND gb2.period_name = g_period
                   AND gcc.remeas_value IN ('1',
                                            'A',
                                            'C')) base_view
         GROUP BY base_view.code_combination_id,
                  base_view.account,
                  base_view.controlled_account_type
        HAVING nvl(SUM(base_view.p_accounted_ytd), 0) != 0 OR nvl(SUM(base_view.r_accounted_ytd), 0) != 0;
    
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
      
        cal_by_ccid(p_init_msg_list => fnd_api.g_false,
                    p_commit        => fnd_api.g_false,
                    x_return_status => x_return_status,
                    x_msg_count     => x_msg_count,
                    x_msg_data      => x_msg_data,
                    p_ccid          => l_code_combination_id,
                    x_results       => l_results);
      
        IF x_return_status <> fnd_api.g_ret_sts_success THEN
          l_error_msg := x_msg_data;
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
        
          l_procedure_status := hw_api.g_ret_sts_warning;
        
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
        
          l_procedure_status := hw_api.g_ret_sts_warning;
        
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
  
    IF l_procedure_status = hw_api.g_ret_sts_warning THEN
      hw_api.stack_message('some records failed in procedure cal_standard. Please see error report.');
    END IF;
  
    x_return_status := hw_api.end_activity(p_pkg_name         => g_pkg_name,
                                           p_api_name         => l_api_name,
                                           p_commit           => p_commit,
                                           p_procedure_status => l_procedure_status,
                                           x_msg_count        => x_msg_count,
                                           x_msg_data         => x_msg_data);
  
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_error,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    WHEN fnd_api.g_exc_unexpected_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_unexp,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    WHEN OTHERS THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_others,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    
  END;

  PROCEDURE cal_777(p_init_msg_list IN VARCHAR2 DEFAULT fnd_api.g_false,
                    p_commit        IN VARCHAR2 DEFAULT fnd_api.g_false,
                    x_return_status OUT NOCOPY VARCHAR2,
                    x_msg_count     OUT NOCOPY NUMBER,
                    x_msg_data      OUT NOCOPY VARCHAR2) IS
  
    l_api_name       CONSTANT VARCHAR2(30) := 'cal_777';
    l_savepoint_name CONSTANT VARCHAR2(30) := 'cal_777';
    l_procedure_status VARCHAR2(30);
  
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
         AND t.status = g_record_status_skip
         AND t.sob_id = g_sob_id
       GROUP BY t.account_777,
                t.account_776,
                t.remeas_type;
  
  BEGIN
  
    x_return_status := hw_api.start_activity(p_pkg_name       => g_pkg_name,
                                             p_api_name       => l_api_name,
                                             p_savepoint_name => l_savepoint_name,
                                             p_init_msg_list  => p_init_msg_list);
  
    hw_api.raise_exception(p_return_status     => x_return_status,
                           px_procedure_status => l_procedure_status);
  
    debug_message('----------------------' || 'Start cal 777' ||
                  '----------------------');
  
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
           AND NOT (gcc.segment5 = ('GX9999') AND
                gcc.segment6 = c_defaut_segment6 AND
                gcc.segment7 = c_defaut_segment7 AND
                gcc.segment8 = c_defaut_segment8 AND
                gcc.segment9 = c_defaut_segment9 AND
                gcc.segment10 = c_defaut_segment10 AND
                gcc.segment11 = c_defaut_segment11);
    
    ELSIF g_reval_type = c_source_translate THEN
    
      OPEN l_data_crs FOR
        SELECT base_view.code_combination_id,
               base_view.account,
               base_view.ccl_account,
               base_view.remeas_value,
               base_view.account_777,
               base_view.account_776,
               base_view.controlled_account_type,
               base_view.controlled_account_type_777
          FROM (SELECT gcc.code_combination_id,
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
                         WHERE t.ccl_account = gcc.translate_account) controlled_account_type_777,
                       0 p_accounted_ytd,
                       
                       ((nvl(gb.begin_balance_dr,
                             0) - nvl(gb.begin_balance_cr,
                                        0)) +
                       (nvl(gb.period_net_dr,
                             0) - nvl(gb.period_net_cr,
                                        0))) r_accounted_ytd
                  FROM gerfp_rev_gcc_v  gcc,
                       gl_balances      gb,
                       gl_sets_of_books sob
                 WHERE gcc.segment1 = g_me_code
                   AND gcc.segment2 = g_le_code
                   AND gcc.segment3 = g_book_type
                   AND gb.code_combination_id = gcc.code_combination_id
                   AND gb.currency_code = sob.currency_code
                   AND gb.set_of_books_id = sob.set_of_books_id
                   AND sob.set_of_books_id = g_sob_id
                   AND gb.period_name = g_period
                   AND gcc.remeas_value IN ('E',
                                            'F',
                                            'I',
                                            '3',
                                            '7')
                   AND NOT (gcc.segment5 = ('GX9999') AND
                        gcc.segment6 = c_defaut_segment6 AND
                        gcc.segment7 = c_defaut_segment7 AND
                        gcc.segment8 = c_defaut_segment8 AND
                        gcc.segment9 = c_defaut_segment9 AND
                        gcc.segment10 = c_defaut_segment10 AND
                        gcc.segment11 = c_defaut_segment11)
                
                UNION ALL
                
                SELECT gcc.code_combination_id,
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
                         WHERE t.ccl_account = gcc.translate_account) controlled_account_type_777,
                       ((nvl(gb2.begin_balance_dr,
                             0) - nvl(gb2.begin_balance_cr,
                                        0)) +
                       (nvl(gb2.period_net_dr,
                             0) - nvl(gb2.period_net_cr,
                                        0))) p_accounted_ytd,
                       
                       0 r_accounted_ytd
                  FROM gerfp_rev_gcc_v  gcc,
                       gl_sets_of_books sob2,
                       gl_balances      gb2
                 WHERE gcc.segment1 = g_me_code
                   AND gcc.segment2 = g_le_code
                   AND gcc.segment3 = g_book_type
                   AND gb2.code_combination_id = gcc.code_combination_id
                   AND gb2.currency_code = sob2.currency_code
                   AND gb2.set_of_books_id = sob2.set_of_books_id
                   AND sob2.set_of_books_id = g_psob_id
                   AND gb2.period_name = g_period
                   AND gcc.remeas_value IN ('E',
                                            'F',
                                            'I',
                                            '3',
                                            '7')
                   AND NOT (gcc.segment5 = ('GX9999') AND
                        gcc.segment6 = c_defaut_segment6 AND
                        gcc.segment7 = c_defaut_segment7 AND
                        gcc.segment8 = c_defaut_segment8 AND
                        gcc.segment9 = c_defaut_segment9 AND
                        gcc.segment10 = c_defaut_segment10 AND
                        gcc.segment11 = c_defaut_segment11)) base_view
         GROUP BY base_view.code_combination_id,
                  base_view.account,
                  base_view.ccl_account,
                  base_view.remeas_value,
                  base_view.account_777,
                  base_view.account_776,
                  base_view.controlled_account_type,
                  base_view.controlled_account_type_777
        HAVING nvl(SUM(base_view.p_accounted_ytd), 0) != 0 OR nvl(SUM(base_view.r_accounted_ytd), 0) != 0;
    
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
      
        cal_by_ccid(p_init_msg_list => fnd_api.g_false,
                    p_commit        => fnd_api.g_false,
                    x_return_status => x_return_status,
                    x_msg_count     => x_msg_count,
                    x_msg_data      => x_msg_data,
                    p_ccid          => l_code_combination_id,
                    x_results       => l_results);
      
        IF x_return_status <> fnd_api.g_ret_sts_success THEN
          l_error_msg := x_msg_data;
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
          l_procedure_status := hw_api.g_ret_sts_warning;
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
        
          l_procedure_status := hw_api.g_ret_sts_warning;
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
      
        validate_account(p_account       => l_account_777_rec.account_777,
                         x_remeas_value  => l_remeas_value,
                         x_return_status => x_return_status);
        IF x_return_status <> fnd_api.g_ret_sts_success THEN
        
          l_error_msg := fnd_msg_pub.get(fnd_msg_pub.g_last,
                                         'F');
          fnd_msg_pub.delete_msg(fnd_msg_pub.count_msg);
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
           AND (gcc.segment5 = 'GX9999' OR
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
             AND (gcc.segment5 = 'GX9999' OR
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
                                  g_sob_currency);
          IF nvl(l_rate,
                 0) = 0 THEN
            l_error_msg := fnd_msg_pub.get(fnd_msg_pub.g_last,
                                           'F');
            fnd_msg_pub.delete_msg(fnd_msg_pub.count_msg);
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
             AND (gcc.segment5 = 'GX9999' OR
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
               AND (gcc.segment5 = 'GX9999' OR
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
      
        debug_message('variance_amount: ' || l_rev_amount);
      
        IF abs(nvl(l_rev_amount,
                   0)) >= nvl(g_threshold_amount,
                              0) THEN
        
          l_rev_amount := round(l_rev_amount,
                                g_precision);
        
          debug_message('rounded FX amount: ' || l_rev_amount);
        
          l_777_concat_segments := g_me_code || '.' || g_le_code || '.' ||
                                   g_book_type || '.' ||
                                   l_account_777_rec.account_777 || '.' ||
                                   'GX9999' || '.' || c_defaut_segment6 || '.' ||
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
          l_procedure_status := hw_api.g_ret_sts_warning;
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
        
          l_procedure_status := hw_api.g_ret_sts_warning;
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
  
    IF l_procedure_status = hw_api.g_ret_sts_warning THEN
      hw_api.stack_message('some records failed in procedure cal_777. Please see error report.');
    END IF;
  
    x_return_status := hw_api.end_activity(p_pkg_name         => g_pkg_name,
                                           p_api_name         => l_api_name,
                                           p_commit           => p_commit,
                                           p_procedure_status => l_procedure_status,
                                           x_msg_count        => x_msg_count,
                                           x_msg_data         => x_msg_data);
  
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_error,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    WHEN fnd_api.g_exc_unexpected_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_unexp,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    WHEN OTHERS THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_others,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    
  END;

  PROCEDURE cal_fx(p_init_msg_list IN VARCHAR2 DEFAULT fnd_api.g_false,
                   p_commit        IN VARCHAR2 DEFAULT fnd_api.g_false,
                   x_return_status OUT NOCOPY VARCHAR2,
                   x_msg_count     OUT NOCOPY NUMBER,
                   x_msg_data      OUT NOCOPY VARCHAR2) IS
  
    l_api_name       CONSTANT VARCHAR2(30) := 'cal_fx';
    l_savepoint_name CONSTANT VARCHAR2(30) := 'cal_fx';
    l_procedure_status VARCHAR2(30);
  
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
  
    l_fx_amount NUMBER;
    l_fx_ccid   NUMBER;
  
  BEGIN
  
    x_return_status := hw_api.start_activity(p_pkg_name       => g_pkg_name,
                                             p_api_name       => l_api_name,
                                             p_savepoint_name => l_savepoint_name,
                                             p_init_msg_list  => p_init_msg_list);
  
    hw_api.raise_exception(p_return_status     => x_return_status,
                           px_procedure_status => l_procedure_status);
  
    debug_message('----------------------' || 'Start cal FX' ||
                  '----------------------');
  
    FOR l_data_rec IN l_data_csr
    LOOP
    
      l_fx_amount := NULL;
      l_fx_ccid   := NULL;
    
      l_fx_amount := l_data_rec.reval_tot;
    
      IF l_data_rec.source = c_source_remeasure THEN
        l_fx_ccid := g_fx_ccid;
      ELSIF l_data_rec.source = c_source_translate THEN
        l_fx_ccid := g_cta_ccid;
      END IF;
    
      debug_message('currency_code=' || l_data_rec.currency_code || '; ' ||
                    'source=' || l_data_rec.source || '; ' || 'fx_amount=' ||
                    l_fx_amount || '; ');
    
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
    
    END LOOP;
  
    x_return_status := hw_api.end_activity(p_pkg_name         => g_pkg_name,
                                           p_api_name         => l_api_name,
                                           p_commit           => p_commit,
                                           p_procedure_status => l_procedure_status,
                                           x_msg_count        => x_msg_count,
                                           x_msg_data         => x_msg_data);
  
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_error,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    WHEN fnd_api.g_exc_unexpected_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_unexp,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    WHEN OTHERS THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_others,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    
  END;

  PROCEDURE insert_gl_interface(p_init_msg_list IN VARCHAR2 DEFAULT fnd_api.g_false,
                                p_commit        IN VARCHAR2 DEFAULT fnd_api.g_false,
                                x_return_status OUT NOCOPY VARCHAR2,
                                x_msg_count     OUT NOCOPY NUMBER,
                                x_msg_data      OUT NOCOPY VARCHAR2,
                                x_group_id      OUT NUMBER) IS
  
    l_api_name       CONSTANT VARCHAR2(30) := 'insert_gl_interface';
    l_savepoint_name CONSTANT VARCHAR2(30) := 'insert_gl_interface';
    l_procedure_status VARCHAR2(30);
  
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
  
    x_return_status := hw_api.start_activity(p_pkg_name       => g_pkg_name,
                                             p_api_name       => l_api_name,
                                             p_savepoint_name => l_savepoint_name,
                                             p_init_msg_list  => p_init_msg_list);
  
    hw_api.raise_exception(p_return_status     => x_return_status,
                           px_procedure_status => l_procedure_status);
  
    debug_message('----------------------' || 'Start insert_gl_interface' ||
                  '----------------------');
  
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
  
    x_return_status := hw_api.end_activity(p_pkg_name         => g_pkg_name,
                                           p_api_name         => l_api_name,
                                           p_commit           => p_commit,
                                           p_procedure_status => l_procedure_status,
                                           x_msg_count        => x_msg_count,
                                           x_msg_data         => x_msg_data);
  
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_error,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    WHEN fnd_api.g_exc_unexpected_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_unexp,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    WHEN OTHERS THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_others,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    
  END;

  PROCEDURE submit_journal_import(p_init_msg_list IN VARCHAR2 DEFAULT fnd_api.g_false,
                                  p_commit        IN VARCHAR2 DEFAULT fnd_api.g_false,
                                  x_return_status OUT NOCOPY VARCHAR2,
                                  x_msg_count     OUT NOCOPY NUMBER,
                                  x_msg_data      OUT NOCOPY VARCHAR2,
                                  p_sob_id        IN NUMBER,
                                  p_group_id      IN NUMBER,
                                  p_user_source   IN VARCHAR2) IS
    l_api_name       CONSTANT VARCHAR2(30) := 'submit_journal_import';
    l_savepoint_name CONSTANT VARCHAR2(30) := 'submit_journal_import';
  
    l_procedure_status VARCHAR2(30);
    v_req_id           NUMBER;
    v_interface_run_id NUMBER;
    v_je_source        VARCHAR2(50);
  
    l_line_count NUMBER;
  
  BEGIN
  
    x_return_status := hw_api.start_activity(p_pkg_name       => g_pkg_name,
                                             p_api_name       => l_api_name,
                                             p_savepoint_name => l_savepoint_name,
                                             p_init_msg_list  => p_init_msg_list);
  
    hw_api.raise_exception(p_return_status     => x_return_status,
                           px_procedure_status => l_procedure_status);
  
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
    
      hw_api.stack_message('No lines in GL Interface for group :' ||
                           p_group_id || ';sob id=' || p_sob_id ||
                           ';user_source=' || p_user_source);
      l_procedure_status := hw_api.g_ret_sts_warning;
      GOTO end_of_procedure;
    
    END IF;
  
    BEGIN
      SELECT je_source_name
        INTO v_je_source
        FROM gl_je_sources
       WHERE user_je_source_name = p_user_source;
    EXCEPTION
    
      WHEN OTHERS THEN
        hw_api.stack_message('Errors in getting source name for user source:' ||
                             p_user_source || '. ' || SQLERRM);
        RAISE fnd_api.g_exc_unexpected_error;
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
       'S',
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
      hw_api.stack_message('Failed to submit GL Import. ' ||
                           fnd_message.get);
      RAISE fnd_api.g_exc_unexpected_error;
    END IF;
  
    <<end_of_procedure>>
    x_return_status := hw_api.end_activity(p_pkg_name         => g_pkg_name,
                                           p_api_name         => l_api_name,
                                           p_commit           => p_commit,
                                           p_procedure_status => l_procedure_status,
                                           x_msg_count        => x_msg_count,
                                           x_msg_data         => x_msg_data);
  
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_error,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    WHEN fnd_api.g_exc_unexpected_error THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_unexp,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    WHEN OTHERS THEN
      x_return_status := hw_api.handle_exceptions(p_pkg_name       => g_pkg_name,
                                                  p_api_name       => l_api_name,
                                                  p_savepoint_name => l_savepoint_name,
                                                  p_exc_name       => hw_api.g_exc_name_others,
                                                  x_msg_count      => x_msg_count,
                                                  x_msg_data       => x_msg_data);
    
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
    l_group_id NUMBER;
  
    l_return_status VARCHAR2(30);
    l_msg_data      VARCHAR2(2000);
    l_msg_count     NUMBER;
  
    l_procedure_status VARCHAR2(30);
  
  BEGIN
  
    hw_conc_utl.log_header;
  
    init_val_ledger(p_init_msg_list => fnd_api.g_false,
                    p_commit        => fnd_api.g_false,
                    x_return_status => l_return_status,
                    x_msg_count     => l_msg_count,
                    x_msg_data      => l_msg_data,
                    p_batch_id      => p_batch_id,
                    p_me_code       => p_me_code,
                    p_le_code       => p_le_code,
                    p_book_type     => p_book_type,
                    p_mrc_type      => p_mrc_type,
                    p_period        => p_period,
                    p_rate_date     => p_rate_date,
                    p_reval_type    => p_reval_type);
    hw_api.raise_exception(p_return_status     => l_return_status,
                           px_procedure_status => l_procedure_status);
  
    cal_standard(p_init_msg_list => fnd_api.g_false,
                 p_commit        => fnd_api.g_false,
                 x_return_status => l_return_status,
                 x_msg_count     => l_msg_count,
                 x_msg_data      => l_msg_data);
  
    hw_api.raise_exception(p_return_status     => l_return_status,
                           px_procedure_status => l_procedure_status);
  
    cal_777(p_init_msg_list => fnd_api.g_false,
            p_commit        => fnd_api.g_false,
            x_return_status => l_return_status,
            x_msg_count     => l_msg_count,
            x_msg_data      => l_msg_data);
  
    hw_api.raise_exception(p_return_status     => l_return_status,
                           px_procedure_status => l_procedure_status);
  
    cal_fx(p_init_msg_list => fnd_api.g_false,
           p_commit        => fnd_api.g_false,
           x_return_status => l_return_status,
           x_msg_count     => l_msg_count,
           x_msg_data      => l_msg_data);
    hw_api.raise_exception(p_return_status     => l_return_status,
                           px_procedure_status => l_procedure_status);
  
    insert_gl_interface(p_init_msg_list => fnd_api.g_false,
                        p_commit        => fnd_api.g_false,
                        x_return_status => l_return_status,
                        x_msg_count     => l_msg_count,
                        x_msg_data      => l_msg_data,
                        x_group_id      => l_group_id);
  
    IF l_return_status = fnd_api.g_ret_sts_success THEN
      update_process_status(p_process_status => g_record_status_processed,
                            p_message        => NULL);
    ELSE
      update_process_status(p_process_status => g_record_status_error,
                            p_message        => l_msg_data);
    END IF;
    ---transation finished here
    COMMIT;
  
    submit_journal_import(p_init_msg_list => fnd_api.g_false,
                          p_commit        => fnd_api.g_false,
                          x_return_status => l_return_status,
                          x_msg_count     => l_msg_count,
                          x_msg_data      => l_msg_data,
                          p_sob_id        => g_sob_id,
                          p_group_id      => l_group_id,
                          p_user_source   => g_reval_type);
    IF l_return_status IN
       (fnd_api.g_ret_sts_error,
        fnd_api.g_ret_sts_unexp_error) THEN
      hw_api.stack_message('Failed to Submit Journal Import, Please take care of data in GL Interface');
    END IF;
  
    hw_api.raise_exception(p_return_status     => l_return_status,
                           px_procedure_status => l_procedure_status);
  
    IF l_procedure_status = hw_api.g_ret_sts_warning THEN
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '1';
      errbuf  := fnd_msg_pub.get(fnd_msg_pub.g_last,
                                 'F');
    END IF;
  
    hw_conc_utl.log_footer;
  
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '1';
      errbuf  := substr(l_msg_data,
                        1,
                        200);
    WHEN fnd_api.g_exc_unexpected_error THEN
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '2';
      errbuf  := substr(l_msg_data,
                        1,
                        200);
    WHEN OTHERS THEN
      fnd_msg_pub.add_exc_msg(p_pkg_name       => g_pkg_name,
                              p_procedure_name => 'reval');
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '2';
      errbuf  := SQLERRM;
      hw_log.debug('Error Stack:' || fnd_global.newline ||
                   dbms_utility.format_error_backtrace);
  END;

  PROCEDURE remeasure_by_user(errbuf      OUT VARCHAR2,
                              retcode     OUT VARCHAR2,
                              p_me_code   IN VARCHAR2,
                              p_le_code   IN VARCHAR2,
                              p_book_type IN VARCHAR2,
                              p_sob_id    IN NUMBER,
                              p_period    IN VARCHAR2,
                              p_rate_date IN VARCHAR2) IS
  
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
    l_request_msg   VARCHAR2(2000);
  
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
  
    hw_conc_utl.log_header;
  
    SELECT gerfp_gl_reval_batch_id_s.nextval INTO l_batch_id FROM dual;
  
    SELECT sob.mrc_sob_type_code
      INTO l_mrc_type
      FROM gl_sets_of_books sob
     WHERE sob.set_of_books_id = p_sob_id;
  
    debug_message('mrc_type:' || l_mrc_type);
  
    IF l_mrc_type IN ('N',
                      'P') THEN
      l_p_sod_id := p_sob_id;
      l_mrc_type := 'P';
    ELSE
    
      SELECT ba.primary_set_of_books_id
        INTO l_p_sod_id
        FROM gl_mc_book_assignments ba
       WHERE ba.reporting_set_of_books_id = p_sob_id;
    END IF;
  
    debug_message('primary sod_id:' || l_p_sod_id);
  
    --submit reval program
  
    FOR l_data_rec IN l_data_csr
    LOOP
    
      IF l_data_rec.export4 = 'LC'
         AND l_mrc_type = 'R' THEN
      
        l_msg_data := 'Remeasurement cannot be run in Reporting Book for Local Currency ledger.';
        hw_api.stack_message(l_msg_data);
        RAISE fnd_api.g_exc_unexpected_error;
      
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
        l_msg_data := 'Failed to submit Revaluation. Errors:' ||
                      fnd_message.get;
        hw_api.stack_message(l_msg_data);
        RAISE fnd_api.g_exc_unexpected_error;
      END IF;
    
    END LOOP;
  
    IF nvl(l_req_id,
           0) = 0 THEN
    
      l_msg_data := 'No Requests raised';
      hw_api.stack_message(l_msg_data);
      RAISE fnd_api.g_exc_error;
    END IF;
  
    l_return_status := fnd_concurrent.wait_for_request(request_id => l_req_id,
                                                       INTERVAL   => 1,
                                                       max_wait   => 60 * 30,
                                                       phase      => l_phase,
                                                       status     => l_status,
                                                       dev_phase  => l_dev_phase,
                                                       dev_status => l_dev_status,
                                                       message    => l_request_msg);
  
    IF l_return_status = TRUE THEN
    
      IF l_phase != 'Completed'
         OR l_status IN ('Cancelled',
                         'Error',
                         'Terminated') THEN
      
        l_msg_data := ('Request failed. Status:' || l_dev_status ||
                      ' Phase:' || l_dev_phase || ' Message:' ||
                      l_request_msg);
        hw_api.stack_message(l_msg_data);
        RAISE fnd_api.g_exc_error;
      
      END IF;
    
    ELSE
    
      l_msg_data := ('WAIT FOR REQUEST FAILED - STATUS UNKNOWN');
      hw_api.stack_message(l_msg_data);
      RAISE fnd_api.g_exc_error;
    
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
      l_msg_data := 'Failed to submit Revaluation Status Report . Errors:' ||
                    fnd_message.get;
      hw_api.stack_message(l_msg_data);
      RAISE fnd_api.g_exc_error;
    END IF;
  
    hw_conc_utl.log_footer;
  
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '1';
      errbuf  := l_msg_data;
    WHEN fnd_api.g_exc_unexpected_error THEN
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '2';
      errbuf  := l_msg_data;
    WHEN OTHERS THEN
      fnd_msg_pub.add_exc_msg(p_pkg_name       => g_pkg_name,
                              p_procedure_name => 'remeasure_by_user');
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '2';
      errbuf  := SQLERRM;
      hw_log.debug('Error Stack:' || fnd_global.newline ||
                   dbms_utility.format_error_backtrace);
  END;

  PROCEDURE translate_by_user(errbuf      OUT VARCHAR2,
                              retcode     OUT VARCHAR2,
                              p_me_code   IN VARCHAR2,
                              p_le_code   IN VARCHAR2,
                              p_book_type IN VARCHAR2,
                              p_sob_id    IN NUMBER,
                              p_period    IN VARCHAR2,
                              p_rate_date IN VARCHAR2) IS
  
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
    l_request_msg   VARCHAR2(2000);
  
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
  
    hw_conc_utl.log_header;
  
    SELECT gerfp_gl_reval_batch_id_s.nextval INTO l_batch_id FROM dual;
  
    SELECT sob.mrc_sob_type_code
      INTO l_mrc_type
      FROM gl_sets_of_books sob
     WHERE sob.set_of_books_id = p_sob_id;
  
    debug_message('mrc_type:' || l_mrc_type);
  
    IF l_mrc_type IN ('N',
                      'P') THEN
      l_p_sod_id := p_sob_id;
      l_mrc_type := 'P';
    ELSE
    
      SELECT ba.primary_set_of_books_id
        INTO l_p_sod_id
        FROM gl_mc_book_assignments ba
       WHERE ba.reporting_set_of_books_id = p_sob_id;
    END IF;
  
    --submit reval program
  
    FOR l_data_rec IN l_data_csr
    LOOP
    
      IF NOT (l_data_rec.export4 = 'LC' AND l_mrc_type = 'R') THEN
      
        l_msg_data := 'Translation only can be run in Reporting Book for Local Currency Ledger. mrc_sob_type_code:' ||
                      l_mrc_type || '; Functional Currency:' ||
                      l_data_rec.export4;
        hw_api.stack_message(l_msg_data);
        RAISE fnd_api.g_exc_unexpected_error;
      
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
        l_msg_data := 'Failed to submit Revaluation. Errors:' ||
                      fnd_message.get;
        hw_api.stack_message(l_msg_data);
        RAISE fnd_api.g_exc_unexpected_error;
      END IF;
    
    END LOOP;
  
    IF nvl(l_req_id,
           0) = 0 THEN
    
      l_msg_data := 'No Requests raised';
      hw_api.stack_message(l_msg_data);
      RAISE fnd_api.g_exc_error;
    END IF;
  
    l_return_status := fnd_concurrent.wait_for_request(request_id => l_req_id,
                                                       INTERVAL   => 1,
                                                       max_wait   => 60 * 30,
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
      
        l_msg_data := ('Request failed. Status:' || l_dev_status ||
                      ' Phase:' || l_dev_phase || ' Message:' ||
                      l_request_msg);
        hw_api.stack_message(l_msg_data);
        RAISE fnd_api.g_exc_error;
      
      END IF;
    
    ELSE
    
      l_msg_data := ('WAIT FOR REQUEST FAILED - STATUS UNKNOWN');
      hw_api.stack_message(l_msg_data);
      RAISE fnd_api.g_exc_error;
    
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
      l_msg_data := 'Failed to submit Revaluation Status Report . Errors:' ||
                    fnd_message.get;
      hw_api.stack_message(l_msg_data);
      RAISE fnd_api.g_exc_error;
    END IF;
  
    hw_conc_utl.log_footer;
  
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '1';
      errbuf  := l_msg_data;
    WHEN fnd_api.g_exc_unexpected_error THEN
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '2';
      errbuf  := l_msg_data;
    WHEN OTHERS THEN
      fnd_msg_pub.add_exc_msg(p_pkg_name       => g_pkg_name,
                              p_procedure_name => 'translate_by_user');
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '2';
      errbuf  := SQLERRM;
      hw_log.debug('Error Stack:' || fnd_global.newline ||
                   dbms_utility.format_error_backtrace);
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
  
    l_batch_id NUMBER;
    l_req_id   NUMBER;
  
    l_phase         VARCHAR2(2000);
    l_status        VARCHAR2(2000);
    l_dev_phase     VARCHAR2(2000);
    l_dev_status    VARCHAR2(2000);
    l_return_status BOOLEAN;
    l_request_msg   VARCHAR2(2000);
    l_msg_data      VARCHAR2(2000);
  
    l_idx NUMBER;
  
    l_user_concurrent_program_name VARCHAR2(2000);
    l_argument_text                VARCHAR2(2000);
    l_request_date                 DATE;
    l_actual_start_date            DATE;
    l_actual_completion_date       DATE;
  
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
  
    hw_conc_utl.log_header;
  
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
    
      l_msg_data := 'No data to process';
      hw_api.stack_message(l_msg_data);
      RAISE fnd_api.g_exc_error;
    END IF;
  
    l_idx := l_request_ids.first;
    <<requests_loop>>
    WHILE l_idx IS NOT NULL
    LOOP
    
      l_return_status := fnd_concurrent.wait_for_request(request_id => l_request_ids(l_idx),
                                                         INTERVAL   => 1,
                                                         max_wait   => 60 * 30,
                                                         phase      => l_phase,
                                                         status     => l_status,
                                                         dev_phase  => l_dev_phase,
                                                         dev_status => l_dev_status,
                                                         message    => l_request_msg);
    
      IF l_return_status = TRUE THEN
      
        SELECT user_concurrent_program_name,
               argument_text,
               request_date,
               actual_start_date,
               actual_completion_date
          INTO l_user_concurrent_program_name,
               l_argument_text,
               l_request_date,
               l_actual_start_date,
               l_actual_completion_date
          FROM fnd_conc_req_summary_v
         WHERE request_id = l_request_ids(l_idx);
      
        debug_message('Request:' || l_request_ids(l_idx) || '|' ||
                      l_user_concurrent_program_name || '(' ||
                      l_argument_text || ')|' || l_phase || '|' ||
                      l_status || '|' || l_request_msg ||
                      '| reqeust date:' ||
                      to_char(l_request_date,
                              'DD-MON-YYYY HH24:MI:SS') ||
                      '|actual start date:' ||
                      to_char(l_actual_start_date,
                              'DD-MON-YYYY HH24:MI:SS') ||
                      '|actual completion date:' ||
                      to_char(l_actual_completion_date,
                              'DD-MON-YYYY HH24:MI:SS') || '|' ||
                      'Elapsed Time:' ||
                      round((l_actual_completion_date -
                            l_actual_start_date) * 1440 * 60));
      
        IF l_phase != 'Completed'
           OR l_status IN ('Cancelled',
                           'Error',
                           'Terminated') THEN
        
          retcode := 1;
        
        END IF;
      
      ELSE
      
        debug_message('WAIT FOR REQUEST:' || l_request_ids(l_idx) ||
                      ' FAILED - STATUS UNKNOWN');
        retcode := 1;
      
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
      l_msg_data := 'Failed to submit Revaluation Status Report . Errors:' ||
                    fnd_message.get;
      hw_api.stack_message(l_msg_data);
      RAISE fnd_api.g_exc_error;
    END IF;
  
    hw_conc_utl.log_footer;
  
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '1';
      errbuf  := l_request_msg;
    WHEN fnd_api.g_exc_unexpected_error THEN
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '2';
      errbuf  := l_request_msg;
    WHEN OTHERS THEN
    
      fnd_msg_pub.add_exc_msg(p_pkg_name       => g_pkg_name,
                              p_procedure_name => 'remeasure');
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      hw_log.debug('Error Stack:' || fnd_global.newline ||
                   dbms_utility.format_error_backtrace);
    
      retcode := '2';
      errbuf  := SQLERRM;
  END;

  PROCEDURE translate(errbuf      OUT VARCHAR2,
                      retcode     OUT VARCHAR2,
                      p_me_code   IN VARCHAR2,
                      p_le_code   IN VARCHAR2,
                      p_book_type IN VARCHAR2,
                      p_sob_id    IN NUMBER,
                      p_period    IN VARCHAR2,
                      p_rate_date IN VARCHAR2) IS
  
    l_batch_id NUMBER;
    l_req_id   NUMBER;
  
    l_phase         VARCHAR2(2000);
    l_status        VARCHAR2(2000);
    l_dev_phase     VARCHAR2(2000);
    l_dev_status    VARCHAR2(2000);
    l_return_status BOOLEAN;
    l_request_msg   VARCHAR2(2000);
    l_msg_data      VARCHAR2(2000);
  
    l_idx NUMBER;
  
    l_user_concurrent_program_name VARCHAR2(2000);
    l_argument_text                VARCHAR2(2000);
    l_request_date                 DATE;
    l_actual_start_date            DATE;
    l_actual_completion_date       DATE;
  
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
  
    hw_conc_utl.log_header;
  
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
    
      l_msg_data := 'No data to process';
      hw_api.stack_message(l_msg_data);
      RAISE fnd_api.g_exc_error;
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
                                                         message    => l_request_msg);
    
      IF l_return_status = TRUE THEN
      
        SELECT user_concurrent_program_name,
               argument_text,
               request_date,
               actual_start_date,
               actual_completion_date
          INTO l_user_concurrent_program_name,
               l_argument_text,
               l_request_date,
               l_actual_start_date,
               l_actual_completion_date
          FROM fnd_conc_req_summary_v
         WHERE request_id = l_request_ids(l_idx);
      
        debug_message('Request:' || l_request_ids(l_idx) || '|' ||
                      l_user_concurrent_program_name || '(' ||
                      l_argument_text || ')|' || l_phase || '|' ||
                      l_status || '|' || l_request_msg ||
                      '| reqeust date:' ||
                      to_char(l_request_date,
                              'DD-MON-YYYY HH24:MI:SS') ||
                      '|actual start date:' ||
                      to_char(l_actual_start_date,
                              'DD-MON-YYYY HH24:MI:SS') ||
                      '|actual completion date:' ||
                      to_char(l_actual_completion_date,
                              'DD-MON-YYYY HH24:MI:SS') || '|' ||
                      'Elapsed Time:' ||
                      round((l_actual_completion_date -
                            l_actual_start_date) * 1440 * 60));
      
        IF l_phase != 'Completed'
           OR l_status IN ('Cancelled',
                           'Error',
                           'Terminated') THEN
        
          retcode := 1;
        
        END IF;
      
      ELSE
      
        debug_message('WAIT FOR REQUEST:' || l_request_ids(l_idx) ||
                      ' FAILED - STATUS UNKNOWN');
        retcode := 1;
      
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
      l_msg_data := 'Failed to submit Revaluation Status Report . Errors:' ||
                    fnd_message.get;
      hw_api.stack_message(l_msg_data);
      RAISE fnd_api.g_exc_error;
    END IF;
  
    hw_conc_utl.log_footer;
  
  EXCEPTION
    WHEN fnd_api.g_exc_error THEN
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '1';
      errbuf  := l_msg_data;
    WHEN fnd_api.g_exc_unexpected_error THEN
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '2';
      errbuf  := l_msg_data;
    WHEN OTHERS THEN
      fnd_msg_pub.add_exc_msg(p_pkg_name       => g_pkg_name,
                              p_procedure_name => 'remeasure');
      hw_conc_utl.log_message_list;
      hw_conc_utl.out_message_list;
      retcode := '2';
      errbuf  := SQLERRM;
      hw_log.debug('Error Stack:' || fnd_global.newline ||
                   dbms_utility.format_error_backtrace);
  END;

END gerfp_gl_reval_pub;
/
