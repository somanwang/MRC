CREATE OR REPLACE PACKAGE gerfp_gl_rev_p2r_pkg AUTHID CURRENT_USER IS
  /************************************************************************************
   *            - Copy Right General Electric Company 2008 -                          *
   *                                                                                  *
   ************************************************************************************
   * Project    :  GERFP Financial Implementation Project
   * Application    :  General Ledger
   * Title    :  GERFP GL Revaluation Customization
   * Program Name   :  GERFP GL Revaluation Customization
   * Description Purpose  :  To Create New journal for the revaluation journal
   * $Revision    :
   * Utility    :
   * Created by   :  Shireesha Kamma
   * Creation Date  :  27-JAN-2009
   * Called By            :  GERFP GL Revaluation Alert
   * Parameters           :  Je header id , Period name , Set of books Id,
           Responsibility Id
   * Dependency   :  N/A
   * Frequency    :  Whenever the Journal Posted
   * Related documents  :
   *
   * Change History :
   *===================================================================================
   * Date       |Name       |Case#    |Remarks            *
   *===================================================================================
   *    |       |   |                   *
   *    |       |   |                   *
   ************************************************************************************
  */
  --------------------------------------------------------------------------------------
  -- PACKAGE SPECIFICATION
  --------------------------------------------------------------------------------------

  PROCEDURE revaluation_validation_prc(errbuf        OUT VARCHAR2,
                                       retcode       OUT VARCHAR2,
                                       p_header_id   IN NUMBER,
                                       p_period_name IN VARCHAR2,
                                       p_sob_id      IN NUMBER,
                                       p_resp_id     IN NUMBER);

END gerfp_gl_rev_p2r_pkg;
/
CREATE OR REPLACE PACKAGE BODY gerfp_gl_rev_p2r_pkg IS
  /************************************************************************************
   *            - Copy Right General Electric Company 2008 -                          *
   *                                                                                  *
   ************************************************************************************
   * Project        :  GERFP Financial Implementation Project
   * Application        :  General Ledger
   * Title        :  GERFP GL Revaluation Customization
   * Program Name        :  GERFP GL Revaluation Customization
   * Description Purpose    :  To create RSOB Revaluation entry
   * $Revision        :
   * Utility        :
   * Created by        :  Shireesha Kamma
   * Creation Date    :  27-JAN-2009
   * Called By            :
   * Parameters           :
   * Dependency        :  N/A
   * Frequency        :  Whenever the Journal Posted
   * Related documents    :
   * Tables/views accessed:
   *
   * Table Name                    SELECT     Insert     Update     Delete
   * ---------------------         ------     ------     ------     ------
   *  GL_JE_HEADERS                     X          -          -          -
   *  GL_JE_LINES                       X          -          X          -
   *  GL_SETS_OF_BOOKS                  X          -          -          -
   *  GL_PERIODS                        X          -          -          -
   *  GL_JE_CATEGORIES                  X          -          -          -
   *  GL_DAILY_CONVERSION_TYPES          X          -          -          -
   *
   *  GL_INTERFACE                      -          X          -          -
   *
   *
   *
   *
   * Change History    :
   *===================================================================================
   * Date             |Name                |Case#        |Remarks                    *
   *===================================================================================
   *  10-MAR-2010     |Shireesha k |        |Fixed the unblanced journal issue  *
   *                  |         |        |(Rev_unbalance_line_new)        *
   * 20-SEP-2010      | Kishore Variganji | 10082901    | Revaluation Rounding Issue  *
   ************************************************************************************
  */
  --------------------------------------------------------------------------------------
  -- PACKAGE BODY
  --------------------------------------------------------------------------------------

  c_defaut_segment5  gl_code_combinations.segment5%TYPE := '000000';
  c_defaut_segment6  gl_code_combinations.segment6%TYPE := '0000000000';
  c_defaut_segment7  gl_code_combinations.segment7%TYPE := '000000';
  c_defaut_segment8  gl_code_combinations.segment8%TYPE := '000000';
  c_defaut_segment9  gl_code_combinations.segment9%TYPE := '000000';
  c_defaut_segment10 gl_code_combinations.segment10%TYPE := '0';
  c_defaut_segment11 gl_code_combinations.segment11%TYPE := '0';

  PROCEDURE debug_message(p_message IN VARCHAR2) IS
  BEGIN
    fnd_file.put_line(fnd_file.log,
                      p_message);
  EXCEPTION
    WHEN OTHERS THEN
      fnd_file.put_line(fnd_file.log,
                        'Error occured in DEBUG_MESSAGE Procedure : ' ||
                        SQLERRM);
  END debug_message;

  /* Procedure to insert deferred journal to GL interface table */

  PROCEDURE reval_iface_insrt(p_sob            IN NUMBER,
                              p_effective_date IN DATE,
                              p_cur_code       IN VARCHAR2,
                              p_category       IN VARCHAR2,
                              p_source         IN VARCHAR2,
                              p_segment1       IN VARCHAR2,
                              p_segment2       IN VARCHAR2,
                              p_segment3       IN VARCHAR2,
                              p_segment4       IN VARCHAR2,
                              p_segment5       IN VARCHAR2,
                              p_segment6       IN VARCHAR2,
                              p_segment7       IN VARCHAR2,
                              p_segment8       IN VARCHAR2,
                              p_segment9       IN VARCHAR2,
                              p_segment10      IN VARCHAR2,
                              p_segment11      IN VARCHAR2,
                              p_entered_dr     IN NUMBER,
                              p_entered_cr     IN NUMBER,
                              p_accounted_dr   IN NUMBER,
                              p_accounted_cr   IN NUMBER,
                              p_reference6     IN VARCHAR2,
                              --    p_cur_conv_date    IN    DATE,
                              --    p_conv_type    IN    VARCHAR2,
                              --    p_conv_rate    IN    NUMBER,
                              p_date_created IN DATE,
                              p_created_by   IN NUMBER,
                              p_group_id     IN NUMBER,
                              p_reference4   IN VARCHAR2,
                              p_reference5   IN VARCHAR2,
                              p_reference10  IN VARCHAR2) IS
  BEGIN
    INSERT INTO gl_interface
      (status,
       set_of_books_id,
       accounting_date,
       currency_code,
       actual_flag,
       user_je_category_name,
       user_je_source_name,
       segment1,
       segment2,
       segment3,
       segment4,
       segment5,
       segment6,
       segment7,
       segment8,
       segment9,
       segment10,
       segment11,
       entered_dr,
       entered_cr,
       accounted_dr,
       accounted_cr,
       reference6,
       --    currency_conversion_date,
       --    user_currency_conversion_type,
       --    currency_conversion_rate,
       date_created,
       created_by,
       group_id,
       reference4,
       reference5,
       reference10,
       reference1)
    VALUES
      ('NEW',
       p_sob,
       p_effective_date,
       p_cur_code,
       'A',
       p_category,
       p_source,
       p_segment1,
       p_segment2,
       p_segment3,
       p_segment4,
       p_segment5,
       p_segment6,
       p_segment7,
       p_segment8,
       p_segment9,
       p_segment10,
       p_segment11,
       decode(p_cur_code,
              'USD',
              p_entered_dr,
              0),
       decode(p_cur_code,
              'USD',
              p_entered_cr,
              0),
       p_accounted_dr,
       p_accounted_cr,
       p_reference6,
       --    p_cur_conv_date,
       --    p_conv_type,
       --    p_conv_rate,
       p_date_created,
       p_created_by,
       p_group_id,
       p_reference4,
       p_reference5,
       p_reference10,
       p_segment1 || '.' || p_segment2);
  EXCEPTION
    WHEN OTHERS THEN
      debug_message('Error occured in REVAL_IFACE_INSRT Procedure : ' ||
                    SQLERRM);
  END reval_iface_insrt;

  PROCEDURE rev_unbalance_line_new(p_sob         IN NUMBER,
                                   p_group_id    IN NUMBER,
                                   p_psob        IN NUMBER,
                                   p_cta_account IN VARCHAR2) IS
    v_acct_dff  NUMBER := 0;
    v_acct_cr   NUMBER := 0;
    v_acct_dr   NUMBER := 0;
    v_segment1  VARCHAR2(6);
    v_segment2  VARCHAR2(6);
    v_segment3  VARCHAR2(1);
    v_segment4  VARCHAR2(13);
    v_segment5  VARCHAR2(6);
    v_segment6  VARCHAR2(10);
    v_segment7  VARCHAR2(6);
    v_segment8  VARCHAR2(6);
    v_segment9  VARCHAR2(6);
    v_segment10 VARCHAR2(1);
    v_segment11 VARCHAR2(1);
  
    lv_segment1  VARCHAR2(6);
    lv_segment2  VARCHAR2(6);
    lv_segment3  VARCHAR2(1);
    lv_segment4  VARCHAR2(13);
    lv_segment5  VARCHAR2(6);
    lv_segment6  VARCHAR2(10);
    lv_segment7  VARCHAR2(6);
    lv_segment8  VARCHAR2(6);
    lv_segment9  VARCHAR2(6);
    lv_segment10 VARCHAR2(1);
    lv_segment11 VARCHAR2(1);
  
    v_ext_ccid  NUMBER;
    v_acct_ccid NUMBER;
    v_rec_cnt   NUMBER := 0;
    v_acct_amt  NUMBER := 0;
    v_grtst_amt NUMBER := 0;
    v_ac_sum_dr NUMBER := 0;
    v_ac_sum_cr NUMBER := 0;
  
    CURSOR l_data_csr IS
    
      SELECT segment1,
             segment2,
             segment3,
             SUM(accounted_dr) - SUM(accounted_cr) v_acct_dff,
             SUM(accounted_dr) v_ac_sum_dr,
             SUM(accounted_cr) v_ac_sum_cr
        FROM gl_interface
       WHERE user_je_category_name = 'Transfer From Primary'
         AND user_je_source_name = 'Remeasurement'
         AND set_of_books_id = p_sob
         AND group_id = p_group_id
       GROUP BY segment1,
                segment2,
                segment3;
  BEGIN
  
    FOR l_data_rec IN l_data_csr
    LOOP
    
      v_ac_sum_dr := l_data_rec.v_acct_dff;
      v_ac_sum_cr := l_data_rec.v_ac_sum_cr;
      v_acct_dff  := l_data_rec.v_acct_dff;
    
      debug_message(' Accounted Diff : ' || v_acct_dff || ' Dr Sum :' ||
                    v_ac_sum_dr || ' Cr Sum :' || v_ac_sum_cr);
    
      IF nvl(v_acct_dff,
             0) <> 0 THEN
      
        debug_message(' RSOB ' || p_sob || ' Group Id ' || p_group_id);
      
        v_acct_dr := -1 * (v_acct_dff);
      
        BEGIN
        
          SELECT l_data_rec.segment1,
                 l_data_rec.segment2,
                 l_data_rec.segment3,
                 p_cta_account,
                 c_defaut_segment5,
                 c_defaut_segment6,
                 c_defaut_segment7,
                 c_defaut_segment8,
                 c_defaut_segment9,
                 c_defaut_segment10,
                 c_defaut_segment11
            INTO v_segment1,
                 v_segment2,
                 v_segment3,
                 v_segment4,
                 v_segment5,
                 v_segment6,
                 v_segment7,
                 v_segment8,
                 v_segment9,
                 v_segment10,
                 v_segment11
            FROM dual;
        
        EXCEPTION
          WHEN OTHERS THEN
            debug_message('ccid check  ' || SQLERRM);
            NULL;
        END;
      
        debug_message('All Segments ' || v_segment1 || '-' || v_segment2 || '-' ||
                      v_segment3 || '-' || v_segment4 || '-' || v_segment5 || '-' ||
                      v_segment6 || '-' || v_segment7 || '-' || v_segment8 || '-' ||
                      v_segment9 || '-' || v_segment10 || '-' ||
                      v_segment11);
      
        INSERT INTO gl_interface
          (status,
           set_of_books_id,
           accounting_date,
           currency_code,
           actual_flag,
           user_je_category_name,
           user_je_source_name,
           segment1,
           segment2,
           segment3,
           segment4,
           segment5,
           segment6,
           segment7,
           segment8,
           segment9,
           segment10,
           segment11,
           entered_dr,
           entered_cr,
           accounted_dr,
           accounted_cr,
           reference6,
           date_created,
           created_by,
           group_id,
           reference4,
           reference5,
           reference10,
           code_combination_id,
           reference1)
          SELECT status,
                 set_of_books_id,
                 accounting_date,
                 currency_code,
                 actual_flag,
                 user_je_category_name,
                 user_je_source_name,
                 v_segment1,
                 v_segment2,
                 v_segment3,
                 v_segment4,
                 v_segment5,
                 v_segment6,
                 v_segment7,
                 v_segment8,
                 v_segment9,
                 v_segment10,
                 v_segment11,
                 
                 decode(currency_code,
                        'USD',
                        v_acct_dr,
                        0),
                 0,
                 v_acct_dr,
                 0,
                 reference6,
                 date_created,
                 created_by,
                 group_id,
                 reference4,
                 reference5,
                 'Rounding Difference', --Difference Line ',    -- reference10,
                 v_ext_ccid, -- rec_bal.code_combination_id                 
                 reference1
            FROM gl_interface
           WHERE user_je_source_name = 'Remeasurement'
             AND user_je_category_name = 'Transfer From Primary'
             AND group_id = p_group_id
             AND set_of_books_id = p_sob
             AND rownum < 2;
      
        COMMIT;
      
      END IF;
    
    END LOOP;
  
  EXCEPTION
    WHEN OTHERS THEN
      debug_message('Error occured in REV_UNBALANCE_LINE_NEW Procedure : ' ||
                    SQLERRM);
  END rev_unbalance_line_new;

  ----------------------------------------------------------------------------------------------------------
  /*   PROCEDURE "GERFP_SUBMIT_IMPORT_PROC" to Submit JOURNAL IMPORT Program */
  ----------------------------------------------------------------------------------------------------------

  PROCEDURE submit_journal_import(p_user_id  IN NUMBER,
                                  p_resp_id  IN NUMBER,
                                  p_sob_id   IN NUMBER,
                                  p_group_id IN NUMBER,
                                  p_source   IN VARCHAR2,
                                  x_status   OUT VARCHAR2) IS
    --  errbuf OUT  VARCHAR2
    --                              ,retcode OUT VARCHAR2
    v_req_id NUMBER;
    --v_user_id           NUMBER;
    v_appl_id NUMBER;
    --v_resp_id           NUMBER;
    --v_set_of_books_id   NUMBER;
    -- v_group_id          NUMBER;
    v_suspense_flag     VARCHAR2(2) := 'Y';
    v_interface_run_id  NUMBER;
    v_req_return_status BOOLEAN;
    v_summary_flag      VARCHAR2(1) := 'N';
    v_source_name       VARCHAR2(30);
    v_user_source_name  VARCHAR2(80);
    v_req_phase         VARCHAR2(30);
    v_req_status        VARCHAR2(30);
    v_req_dev_phase     VARCHAR2(30);
    v_req_dev_status    VARCHAR2(30);
    v_req_message       VARCHAR2(50);
    v_je_source         VARCHAR2(50);
  
  BEGIN
  
    SELECT DISTINCT application_id
      INTO v_appl_id
      FROM fnd_application
     WHERE application_short_name = 'SQLGL';
  
    -------------------------------------------------------------------------------------
    -- Initilization of APPS Application using FND_GLOBAL Procedure
    -------------------------------------------------------------------------------------
  
    --  FND_GLOBAL.APPS_INITIALIZE(V_user_id, V_resp_id, V_appl_id);
    fnd_global.apps_initialize(p_user_id,
                               p_resp_id,
                               v_appl_id);
  
    /*Deriving the Source Name*/
    BEGIN
    
      SELECT je_source_name
        INTO v_je_source
        FROM gl_je_sources
       WHERE user_je_source_name = p_source;
    
    EXCEPTION
      WHEN no_data_found THEN
        fnd_file.put_line(fnd_file.log,
                          'No Data Found Exception : JE_SOURCE_NAME does not exist for SHELTON');
      WHEN too_many_rows THEN
        fnd_file.put_line(fnd_file.log,
                          'Exact Fetch Returned Too many Rows while extracting JE_SOURCE_NAME Value');
      WHEN OTHERS THEN
        fnd_file.put_line(fnd_file.log,
                          'SQL ERROR MESSAGE while extracting JE_SOURCE_NAME Value:' ||
                          SQLERRM);
    END;
  
    /* Sequence to create RUN_ID -- -- GERFP_IMPORT_RUN_ID_S.NEXTVAL */
    SELECT gl_journal_import_s.nextval INTO v_interface_run_id FROM dual;
  
    debug_message('Before insert to Interface control table  ');
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
      (v_je_source --p_source
      ,
       'S',
       v_interface_run_id,
       p_group_id,
       p_sob_id,
       NULL,
       v_req_id);
    COMMIT;
  
    debug_message('After insert to Interface control table  ');
  
    --     Calling FND_REQUEST for Journal Import
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
  
    debug_message('Import Request id :' || v_req_id);
  
    IF v_req_id = 0 THEN
      raise_application_error(-20160,
                              fnd_message.get);
      x_status := 'Failed';
    ELSE
      x_status := 'Done';
    END IF;
  
  EXCEPTION
    WHEN OTHERS THEN
      fnd_file.put_line(fnd_file.log,
                        'JOB FAILED' || SQLERRM);
  END submit_journal_import;

  /* Fence validation procedure ( Main Program )*/

  PROCEDURE revaluation_validation_prc(errbuf        OUT VARCHAR2,
                                       retcode       OUT VARCHAR2,
                                       p_header_id   IN NUMBER,
                                       p_period_name IN VARCHAR2,
                                       p_sob_id      IN NUMBER,
                                       p_resp_id     IN NUMBER) IS
  
    /*  Declaration of  Local Variables */
  
    c_segment4        VARCHAR2(13) := '7400010026701'; -- Account
    v_segment1        VARCHAR2(6);
    v_segment2        VARCHAR2(6);
    v_segment3        VARCHAR2(1);
    v_segment4        VARCHAR2(13);
    v_segment5        VARCHAR2(6);
    v_segment6        VARCHAR2(10);
    v_segment7        VARCHAR2(6);
    v_segment8        VARCHAR2(6);
    v_segment9        VARCHAR2(6);
    v_segment10       VARCHAR2(1);
    v_segment11       VARCHAR2(1);
    c_je_source       VARCHAR2(50);
    v_rsob_id         NUMBER;
    v_psob_id         NUMBER;
    v_userid          NUMBER;
    v_group_id        NUMBER := 0;
    v_reference4      VARCHAR2(100);
    v_currency_flag   VARCHAR2(20);
    v_sob_name        gl_sets_of_books.name%TYPE;
    v_import_status   VARCHAR2(500);
    v_intr_rec_cnt    NUMBER := 0;
    v_entered_dr      NUMBER := 0;
    v_entered_cr      NUMBER := 0;
    v_accounted_dr    NUMBER(25, 2) := 0;
    v_accounted_cr    NUMBER(25, 2) := 0;
    v_conversion_rate gl_daily_rates.conversion_rate%TYPE;
  
    /*  Declaration of User Defined Exceptions */
    e_end_program EXCEPTION;
  
    l_fx_enable_flag  VARCHAR2(100);
    l_bus_enable_flag VARCHAR2(100);
    l_cta_account     VARCHAR2(100);
    l_fx_account      VARCHAR2(100);
    l_fx_cc           VARCHAR2(100);
    l_fx_project      VARCHAR2(100);
    l_fx_reference    VARCHAR2(100);
    l_fc              VARCHAR2(100);
  
    /* Declaration of Journal Header  Cursor */
  
    CURSOR cur_je_data IS
      SELECT gjh.je_header_id,
             gjh.set_of_books_id,
             gjh.je_category,
             gjh.je_source,
             gjh.period_name,
             gjh.name,
             gjh.currency_code,
             gjh.actual_flag,
             gjh.default_effective_date,
             gjh.description,
             gjh.currency_conversion_rate,
             gdct.user_conversion_type          currency_conversion_type,
             gjh.currency_conversion_date,
             gjh.date_created,
             gjh.posted_date,
             gjh.last_updated_by                userid,
             gjh.reversed_je_header_id,
             gjh.doc_sequence_value,
             gjc.user_je_category_name          je_category_name,
             gjs.user_je_source_name            je_source_name,
             gjh.status,
             gjl.je_line_num,
             gjl.code_combination_id,
             gcc.segment1                       segment1, -- ME,
             gcc.segment2                       segment2, -- LE,
             gcc.segment3                       segment3, -- Book_type,
             gcc.segment4                       segment4, -- Account,
             gcc.segment5                       segment5, -- Cost_center,
             gcc.segment6                       segment6, -- Project,
             gcc.segment7                       segment7, -- IME,
             gcc.segment8                       segment8, -- ILE,
             gcc.segment9                       segment9, -- Reference
             gcc.segment10                      segment10, -- Future1,
             gcc.segment11                      segment11, -- Future2,
             gjl.entered_dr,
             gjl.entered_cr,
             gjl.accounted_dr,
             gjl.accounted_cr,
             gjl.description                    line_description,
             gcc.account_type,
             gsob.name                          sob_name,
             gsob.currency_code                 psob_currency,
             gsob.set_of_books_id               psob_id,
             gsob.chart_of_accounts_id,
             rsob.set_of_books_id               rsob_id,
             rsob.currency_code                 rsob_currency,
             rsob.name                          rsob_name,
             gsob.cum_trans_code_combination_id
        FROM apps.gl_je_headers             gjh,
             apps.gl_je_categories          gjc,
             apps.gl_je_sources             gjs,
             apps.gl_daily_conversion_types gdct,
             apps.gl_je_lines               gjl,
             apps.gl_code_combinations      gcc,
             apps.gl_sets_of_books          gsob,
             apps.gl_mc_book_assignments    msob,
             apps.gl_sets_of_books          rsob
       WHERE gjh.je_header_id = gjl.je_header_id
         AND gjl.code_combination_id = gcc.code_combination_id
         AND gjh.currency_conversion_type = gdct.conversion_type
         AND gjh.status = 'P'
         AND gjh.je_category = gjc.je_category_name
         AND gjh.je_source = gjs.je_source_name
         AND gjh.set_of_books_id = gsob.set_of_books_id
         AND gsob.set_of_books_id = msob.primary_set_of_books_id
         AND rsob.set_of_books_id = msob.reporting_set_of_books_id
         AND gsob.mrc_sob_type_code = 'P'
         AND gjs.user_je_source_name = 'Remeasurement'
         AND gjc.user_je_category_name = 'Remeasurement'
         AND gjh.set_of_books_id = p_sob_id
         AND gjh.je_header_id = p_header_id;
  
  BEGIN
  
    /* Getting Userid value from profile */
  
    v_userid := to_number(fnd_profile.value('USER_ID'));
  
    fnd_file.put_line(fnd_file.log,
                      '                                 ');
    fnd_file.put_line(fnd_file.log,
                      'Parameter List of Revaluation Program  ');
    fnd_file.put_line(fnd_file.log,
                      '****************************************');
    fnd_file.put_line(fnd_file.log,
                      ' Set of Books Id    : ' || p_sob_id);
    fnd_file.put_line(fnd_file.log,
                      ' Period Name        : ' || p_period_name);
    fnd_file.put_line(fnd_file.log,
                      ' Responsibility Id    : ' || p_resp_id);
    fnd_file.put_line(fnd_file.log,
                      '                                        ');
  
    debug_message('+----------------------------------------------------------+ ');
  
    debug_message(' Starting... ');
  
    /* Looping the journal header  data */
    debug_message('+--------------------------------------------------+');
  
    /* Group Id Generation */
  
    BEGIN
      -- SELECT  TO_CHAR(SYSDATE,'DDMMYYHH24MISS')||GERFP_GL_REV_SEQ.NEXTVAL INTO v_group_id FROM  DUAL;
      SELECT gerfp_gl_rev_seq.nextval INTO v_group_id FROM dual;
    EXCEPTION
      WHEN no_data_found THEN
        v_group_id := 1;
      WHEN OTHERS THEN
        v_group_id := 1;
    END;
  
    FOR rec_je_data IN cur_je_data
    LOOP
      EXIT WHEN cur_je_data%NOTFOUND;
    
      /* Initialization all local variables */
      v_segment1        := NULL;
      v_segment2        := NULL;
      v_segment3        := NULL;
      v_segment4        := NULL;
      v_segment5        := NULL;
      v_segment6        := NULL;
      v_segment7        := NULL;
      v_segment8        := NULL;
      v_segment9        := NULL;
      v_segment10       := NULL;
      v_segment11       := NULL;
      v_currency_flag   := NULL;
      v_entered_dr      := 0;
      v_entered_cr      := 0;
      v_accounted_dr    := 0;
      v_accounted_cr    := 0;
      v_conversion_rate := 0;
    
      c_je_source := rec_je_data.je_source_name;
      v_rsob_id   := rec_je_data.rsob_id;
      v_psob_id   := rec_je_data.psob_id;
    
      fnd_file.put_line(fnd_file.log,
                        '                                 ');
      fnd_file.put_line(fnd_file.log,
                        ' Journal Name    :' || rec_je_data.name);
      fnd_file.put_line(fnd_file.log,
                        ' Posted Date    :' || rec_je_data.posted_date);
    
      debug_message('------------------------------------------------------------------------------');
      debug_message(' Journal Name  :' || rec_je_data.name ||
                    '  JE Header :' || rec_je_data.je_header_id ||
                    '  Line Number :' || rec_je_data.je_line_num);
      debug_message('------------------------------------------------------------------------------');
    
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
               l_fc,
               l_bus_enable_flag
          FROM xxrfp_shelton_bus_map bus
         WHERE bus.primary_flag = 'Y'
           AND bus.le_code != '000000'
           AND bus.me_code = rec_je_data.segment1
           AND bus.le_code = rec_je_data.segment2
           AND bus.book_type = rec_je_data.segment3;
      
        debug_message('fx_enable_flag=' || l_fx_enable_flag ||
                      ';cta_account=' || l_cta_account || ';fx_account=' ||
                      l_fx_account || ';fx_cc=' || l_fx_cc ||
                      ';fx_project=' || l_fx_project || ';fx_reference=' ||
                      l_fx_reference || ';functional currency=' || l_fc ||
                      ';bus_enable_flag=' || l_bus_enable_flag);
      
      EXCEPTION
        WHEN OTHERS THEN
          NULL;
      END;
    
      IF l_fc = 'LC' THEN
      
        --IF rec_je_data.currency_code=rec_je_data.rsob_currency THEN     -- Commented on 04-FEB-2009
      
        BEGIN
          SELECT conversion_rate
            INTO v_conversion_rate
            FROM gl_daily_rates            gdr,
                 gl_daily_conversion_types gdct
           WHERE gdr.conversion_type = gdct.conversion_type
                -- AND    gdr.status_code='O'
             AND gdct.user_conversion_type = 'GLMOR'
             AND conversion_date = rec_je_data.default_effective_date
             AND from_currency = rec_je_data.psob_currency
             AND to_currency = rec_je_data.rsob_currency;
        
          debug_message('Conversion rate :' || v_conversion_rate ||
                        ' for Conversion_Type : MOR on Date :' ||
                        rec_je_data.default_effective_date ||
                        '  From Currency :' || rec_je_data.psob_currency ||
                        '  To Currency :' || rec_je_data.rsob_currency);
        
          v_accounted_dr := round(nvl(rec_je_data.accounted_dr,
                                      0) * v_conversion_rate,
                                  2);
          v_accounted_cr := round(nvl(rec_je_data.accounted_cr,
                                      0) * v_conversion_rate,
                                  2);
        
          -- v_accounted_dr := NVL(rec_je_data.accounted_dr,0)*v_conversion_rate;
          -- v_accounted_cr := NVL(rec_je_data.accounted_cr,0)*v_conversion_rate;
        
          debug_message('Accounted Dr :' || v_accounted_dr ||
                        ' --> Accounted Cr ' || v_accounted_cr);
        
        EXCEPTION
          WHEN no_data_found THEN
            debug_message(' No conversion rate for Conversion_Type : MOR on Date :' ||
                          rec_je_data.default_effective_date ||
                          '  From Currency :' || rec_je_data.psob_currency ||
                          '  To Currency :' || rec_je_data.rsob_currency);
            RAISE e_end_program;
          WHEN too_many_rows THEN
            debug_message(' More than one conversion rate for Conversion_Type : MOR on Date :' ||
                          rec_je_data.default_effective_date ||
                          '  From Currency :' || rec_je_data.psob_currency ||
                          '  To Currency :' || rec_je_data.rsob_currency);
            RAISE e_end_program;
          WHEN OTHERS THEN
            debug_message(' Getting Conversion rate failed for Conversion_Type : MOR on Date :' ||
                          rec_je_data.default_effective_date ||
                          '  From Currency :' || rec_je_data.psob_currency ||
                          '  To Currency :' || rec_je_data.rsob_currency ||
                          ' -->' || SQLERRM);
            RAISE e_end_program;
        END;
      
        IF rec_je_data.segment4 <> l_fx_account THEN
          BEGIN
            SELECT rec_je_data.segment1,
                   rec_je_data.segment2,
                   rec_je_data.segment3,
                   l_cta_account,
                   c_defaut_segment5,
                   c_defaut_segment6,
                   c_defaut_segment7,
                   c_defaut_segment8,
                   c_defaut_segment9,
                   c_defaut_segment10,
                   c_defaut_segment11
              INTO v_segment1,
                   v_segment2,
                   v_segment3,
                   v_segment4,
                   v_segment5,
                   v_segment6,
                   v_segment7,
                   v_segment8,
                   v_segment9,
                   v_segment10,
                   v_segment11
              FROM dual;
          
          EXCEPTION
            WHEN OTHERS THEN
              NULL;
          END;
        
        ELSE
          v_segment1  := rec_je_data.segment1;
          v_segment2  := rec_je_data.segment2;
          v_segment3  := rec_je_data.segment3;
          v_segment4  := rec_je_data.segment4;
          v_segment5  := rec_je_data.segment5;
          v_segment6  := rec_je_data.segment6;
          v_segment7  := rec_je_data.segment7;
          v_segment8  := rec_je_data.segment8;
          v_segment9  := rec_je_data.segment9;
          v_segment10 := rec_je_data.segment10;
          v_segment11 := rec_je_data.segment11;
        
        END IF;
      
        debug_message(' Insert line to interface table for account ' ||
                      v_segment1 || '.' || v_segment2 || '.' || v_segment3 || '.' ||
                      v_segment4 || '.' || v_segment5 || '.' || v_segment6 || '.' ||
                      v_segment7 || '.' || v_segment8 || '.' || v_segment9 || '.' ||
                      v_segment10 || '.' || v_segment11 || '   Dr ' ||
                      v_accounted_dr || ' Cr ' || v_accounted_cr);
      
        /* Calling procedure to insert journal to interface table */
        reval_iface_insrt(p_sob            => rec_je_data.rsob_id, --set_of_books_id,
                          p_effective_date => rec_je_data.default_effective_date,
                          p_cur_code       => rec_je_data.currency_code,
                          p_category       => 'Transfer From Primary',
                          p_source         => rec_je_data.je_source_name,
                          p_segment1       => v_segment1,
                          p_segment2       => v_segment2,
                          p_segment3       => v_segment3,
                          p_segment4       => v_segment4,
                          p_segment5       => v_segment5,
                          p_segment6       => v_segment6,
                          p_segment7       => v_segment7,
                          p_segment8       => v_segment8,
                          p_segment9       => v_segment9,
                          p_segment10      => v_segment10,
                          p_segment11      => v_segment11,
                          p_entered_dr     => v_accounted_dr, --rec_je_data.entered_dr,
                          p_entered_cr     => v_accounted_cr, -- rec_je_data.entered_cr,
                          p_accounted_dr   => v_accounted_dr, -- rec_je_data.accounted_dr,
                          p_accounted_cr   => v_accounted_cr, -- rec_je_data.accounted_cr,
                          p_reference6     => rec_je_data.je_category || '|' ||
                                              rec_je_data.set_of_books_id || '|' ||
                                              rec_je_data.doc_sequence_value || '|' ||
                                              rec_je_data.je_header_id, --'RSOB REVALUATION JOURNAL'
                          --    p_cur_conv_date    =>    rec_je_data.currency_conversion_date,
                          --    p_conv_type    =>    rec_je_data.currency_conversion_type,
                          --    p_conv_rate    =>    rec_je_data.currency_conversion_rate,
                          p_date_created => rec_je_data.date_created,
                          p_created_by   => v_userid,
                          p_group_id     => v_group_id,
                          p_reference4   => rec_je_data.name,
                          p_reference5   => rec_je_data.description,
                          p_reference10  => 'From Primary book remeasurement for local currency functional ledger. original je_line_num |' ||
                                            rec_je_data.je_line_num);
        --END IF;  -- Commented on 04-FEB-2009
        COMMIT;
      END IF;
    
    END LOOP;
    COMMIT;
    -- Commented on 10-MAR-2010 to fix the unbalance bug
    /* Rev_unbalance_line(p_sob        =>    v_rsob_id,
    p_group_id        =>    v_group_id,
    p_psob        =>    v_psob_id) ; */
    -- Commented on 10-MAR-2010 till here
    -- Added on 10-MAR-2010 to fix the unbalance bug
    rev_unbalance_line_new(p_sob         => v_rsob_id,
                           p_group_id    => v_group_id,
                           p_psob        => v_psob_id,
                           p_cta_account => l_cta_account);
  
    debug_message(' Revaluation Program Completed ');
  
    debug_message(' ------------------------------------------------------------------ ');
  
    debug_message('Group Id  :' || v_group_id);
    debug_message('Souce     :' || c_je_source);
  
    debug_message(' Checking Records in Interface Table ');
    BEGIN
      SELECT COUNT(1)
        INTO v_intr_rec_cnt
        FROM gl_interface
       WHERE user_je_source_name = c_je_source
         AND group_id = v_group_id;
    
      IF nvl(v_intr_rec_cnt,
             0) = 0 THEN
        debug_message('No Records in Interface Table for Source  :' ||
                      c_je_source);
        RAISE e_end_program;
      ELSE
      
        debug_message('No of Records in Interface Table for Source  :' ||
                      c_je_source || ' is ' ||
                      nvl(v_intr_rec_cnt,
                          0));
        debug_message(' Submit JOURNAL IMPORT Program                                      ');
      
        submit_journal_import(p_user_id  => v_userid,
                              p_resp_id  => p_resp_id,
                              p_sob_id   => v_rsob_id,
                              p_group_id => v_group_id,
                              p_source   => c_je_source,
                              x_status   => v_import_status);
      
        debug_message(' JOURNAL IMPORT Program Status : ' ||
                      v_import_status);
      END IF;
    END;
    v_group_id := NULL;
    debug_message(' ------------------------------------------------------------------ ');
  
  EXCEPTION
    WHEN e_end_program THEN
      -- debug_message(' No Journal Lines are Created ');
      retcode := '1';
      --retcode := '2';
    WHEN OTHERS THEN
      debug_message(' Error occured in REVALUATION_VALIDATION_PRC Procedure : ' ||
                    to_char(SQLCODE) || '-' || SQLERRM);
      retcode := '2';
  END revaluation_validation_prc;
END gerfp_gl_rev_p2r_pkg;
/
