CREATE OR REPLACE PACKAGE gerfp_monthend IS
  --
  -- To modify this template, edit file PKGSPEC.TXT in TEMPLATE 
  -- directory of SQL Navigator
  --
  -- Purpose: Briefly explain the functionality of the package
  --
  -- MODIFICATION HISTORY
  -- Person      Date    Comments
  -- ---------   ------  ------------------------------------------       
  -- Enter package declarations as shown below
  PROCEDURE launchqueue(errbuff     OUT VARCHAR2,
                        retcode     OUT VARCHAR2,
                        p_period    IN VARCHAR2,
                        p_rate_date IN VARCHAR2);

  PROCEDURE send_status_report(p_user_id    IN NUMBER,
                               p_recipients IN VARCHAR2,
                               p_stat       OUT VARCHAR2,
                               p_msg        OUT VARCHAR2);

/* 
   FUNCTION function_name
     ( param1 IN datatype DEFAULT default_value, 
       param2 IN OUT datatype)
     RETURN  datatype;
*/
END; -- Package spec
/
CREATE OR REPLACE PACKAGE BODY gerfp_monthend
--GERFP_ME_LAUNCHQUEUE
 IS
  --
  -- To modify this template, edit file PKGBODY.TXT in TEMPLATE 
  -- directory of SQL Navigator
  --
  -- Purpose: Briefly explain the functionality of the package body
  --
  -- MODIFICATION HISTORY
  -- Person      Date    Comments
  -- ---------   ------  ------------------------------------------      
  -- Enter procedure, function bodies as shown below

  v_conc_request NUMBER := fnd_global.conc_request_id;

  PROCEDURE wait_for_requests(p_req_id IN VARCHAR2) IS
  
    v_user_id          NUMBER;
    v_req_id           NUMBER;
    v_request_complete BOOLEAN;
    v_phase            VARCHAR2(20);
    v_status           VARCHAR2(20);
    v_dev_phase        VARCHAR2(20);
    v_dev_status       VARCHAR2(20);
    v_message          VARCHAR2(200);
  
    v_last_req NUMBER;
  
    CURSOR c_queue(p_user_id NUMBER,
                   pc_req_id NUMBER) IS
    --select distinct REQUEST_ID from fnd_concurrent_requests 
      SELECT request_id,
             user_concurrent_program_name,
             argument_text,
             request_date,
             requested_start_date,
             actual_start_date,
             actual_completion_date
        FROM fnd_conc_req_summary_v
       WHERE
      --nvl(PHASE_CODE,'N')<>'C' and 
       requested_by = p_user_id
       AND request_id > pc_req_id
       ORDER BY request_id;
  
  BEGIN
  
    fnd_file.put_line(fnd_file.log,
                      'Quene check begins.');
  
    v_req_id := p_req_id;
    apps.fnd_profile.get('USER_ID',
                         v_user_id);
  
    LOOP
    
      v_last_req := v_req_id;
    
      IF nvl(v_last_req,
             0) = 0 THEN
        EXIT;
      END IF;
    
      dbms_lock.sleep(60);
    
      v_req_id := NULL;
    
      FOR v_request IN c_queue(v_user_id,
                               v_last_req)
      LOOP
      
        v_req_id := v_request.request_id;
        LOOP
          v_phase      := NULL;
          v_status     := NULL;
          v_dev_phase  := NULL;
          v_dev_status := NULL;
          v_message    := NULL;
        
          v_request_complete := apps.fnd_concurrent.wait_for_request(v_req_id,
                                                                     10,
                                                                     9999,
                                                                     v_phase,
                                                                     v_status,
                                                                     v_dev_phase,
                                                                     v_dev_status,
                                                                     v_message);
        
          IF upper(v_phase) = 'COMPLETED' THEN
            fnd_file.put_line(fnd_file.log,
                              'Request:' || v_req_id || '|' ||
                              v_request.user_concurrent_program_name || '(' ||
                              v_request.argument_text || ')|' || v_phase || '|' ||
                              v_status || '|' || v_request.request_date || '|' ||
                              v_request.requested_start_date || '|' ||
                              v_request.actual_start_date || '|' ||
                              v_request.actual_completion_date || '|' ||
                              to_char(SYSDATE,
                                      'DD-MON-YYYY HH24:MI:SS'));
            EXIT;
          END IF;
        END LOOP;
      END LOOP;
    END LOOP;
  
    fnd_file.put_line(fnd_file.log,
                      'Quene check ends.');
  
  END;

  PROCEDURE remeasure(p_period    IN VARCHAR2,
                      p_rate_date IN VARCHAR2) IS
  
    v_req_id NUMBER;
    x_status VARCHAR2(20);
  
  BEGIN
    --P book Post
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Remeasurement begin.-----------------------');
    v_req_id := fnd_request.submit_request(application => 'XXRFP',
                                           program     => 'GERFPGRM',
                                           description => NULL,
                                           start_time  => SYSDATE,
                                           sub_request => FALSE,
                                           argument1   => NULL,
                                           argument2   => NULL,
                                           argument3   => NULL,
                                           argument4   => NULL,
                                           argument5   => p_period,
                                           argument6   => p_rate_date);
    COMMIT;
    IF v_req_id = 0 THEN
      fnd_file.put_line(fnd_file.log,
                        fnd_message.get);
      x_status := 'FAILED';
    ELSE
      x_status := 'DONE';
      fnd_file.put_line(fnd_file.log,
                        'Remeasurement Request: ' || v_req_id || '|' ||
                        x_status || '|' ||
                        to_char(SYSDATE,
                                'DD-MON-YYYY HH24:MI:SS'));
      wait_for_requests(v_req_id);
    
    END IF;
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Remeasurement end.-----------------------');
  END;

  PROCEDURE translate(p_period    IN VARCHAR2,
                      p_rate_date IN VARCHAR2) IS
  
    v_req_id NUMBER;
    x_status VARCHAR2(20);
  
  BEGIN
    --P book Post
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Translation begin.-----------------------');
    v_req_id := fnd_request.submit_request(application => 'XXRFP',
                                           program     => 'GERFPGTR',
                                           description => NULL,
                                           start_time  => SYSDATE,
                                           sub_request => FALSE,
                                           argument1   => NULL,
                                           argument2   => NULL,
                                           argument3   => NULL,
                                           argument4   => NULL,
                                           argument5   => p_period,
                                           argument6   => p_rate_date);
    COMMIT;
    IF v_req_id = 0 THEN
      fnd_file.put_line(fnd_file.log,
                        fnd_message.get);
      x_status := 'FAILED';
    ELSE
      x_status := 'DONE';
      fnd_file.put_line(fnd_file.log,
                        'Remeasurement Request: ' || v_req_id || '|' ||
                        x_status || '|' ||
                        to_char(SYSDATE,
                                'DD-MON-YYYY HH24:MI:SS'));
      wait_for_requests(v_req_id);
    
    END IF;
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Translation end.-----------------------');
  END;

  PROCEDURE ustax(p_period    IN VARCHAR2,
                  p_rate_date IN VARCHAR2,
                  p_us_year   IN VARCHAR2,
                  p_us_date   IN VARCHAR2) IS
  
    v_req_id NUMBER;
    x_status VARCHAR2(20);
  
  BEGIN
    --P book Post
    fnd_file.put_line(fnd_file.log,
                      '-----------------------USTAX begin.-----------------------');
    v_req_id := fnd_request.submit_request(application => 'SQLGL',
                                           program     => 'GERFP_AUTO_RUN_US_TAX',
                                           description => NULL,
                                           start_time  => SYSDATE,
                                           sub_request => FALSE,
                                           argument1   => p_us_year,
                                           argument2   => upper(p_period),
                                           argument3   => p_us_date);
    COMMIT;
    IF v_req_id = 0 THEN
      raise_application_error(-20160,
                              fnd_message.get);
      x_status := 'FAILED';
    ELSE
      x_status := 'DONE';
      fnd_file.put_line(fnd_file.log,
                        'Remeasurement Request: ' || v_req_id || '|' ||
                        x_status || '|' ||
                        to_char(SYSDATE,
                                'DD-MON-YYYY HH24:MI:SS'));
      wait_for_requests(v_req_id);
    
    END IF;
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------USTAX end.-----------------------');
  END;

  PROCEDURE posting(p_mrc_type IN VARCHAR2) IS
  
    v_req_id NUMBER;
    x_status VARCHAR2(20);
  
  BEGIN
    --P book Post
    fnd_file.put_line(fnd_file.log,
                      '-----------------------' || p_mrc_type ||
                      ' Book Post begin.-----------------------');
    v_req_id := fnd_request.submit_request(application => 'SQLGL',
                                           program     => 'GERFP_GL_AUTOPOSTING',
                                           description => NULL,
                                           start_time  => SYSDATE,
                                           sub_request => FALSE,
                                           argument1   => p_mrc_type,
                                           argument2   => NULL);
    COMMIT;
    IF v_req_id = 0 THEN
      raise_application_error(-20160,
                              fnd_message.get);
      x_status := 'FAILED';
    ELSE
      x_status := 'DONE';
      fnd_file.put_line(fnd_file.log,
                        'AUTOPOST Request: ' || v_req_id || '|' || x_status || '|' ||
                        to_char(SYSDATE,
                                'DD-MON-YYYY HH24:MI:SS'));
      wait_for_requests(v_req_id);
    
    END IF;
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------' || p_mrc_type ||
                      ' Book Post end.-----------------------');
  END;

  PROCEDURE launchqueue(errbuff     OUT VARCHAR2,
                        retcode     OUT VARCHAR2,
                        p_period    IN VARCHAR2,
                        p_rate_date IN VARCHAR2) IS
    v_user_id          NUMBER;
    v_req_id           NUMBER;
    v_request_complete BOOLEAN;
    v_phase            VARCHAR2(20);
    v_status           VARCHAR2(20);
    v_dev_phase        VARCHAR2(20);
    v_dev_status       VARCHAR2(20);
    v_message          VARCHAR2(200);
  
    v_last_req NUMBER;
  
    x_status VARCHAR2(20);
  
    v_us_year VARCHAR2(20);
    v_us_date VARCHAR2(20);
    e_invalid_period EXCEPTION;
  
    v_stat       VARCHAR2(20);
    v_msg        VARCHAR2(20);
    v_start_time NUMBER;
  
  BEGIN
    x_status := NULL;
  
    fnd_global.set_nls_context(p_nls_language => 'AMERICAN');
    apps.fnd_profile.get('USER_ID',
                         v_user_id);
    fnd_file.put_line(fnd_file.log,
                      'The master request id is ' || v_conc_request);
  
    --Validate the input parameter: period      
    BEGIN
      SELECT DISTINCT period_year,
                      '15-' || period_name
        INTO v_us_year,
             v_us_date
        FROM gl_period_statuses
       WHERE application_id = 101
         AND closing_status IN ('O')
         AND period_type = '21'
         AND adjustment_period_flag <> 'Y'
         AND period_name = upper(p_period);
    
    EXCEPTION
      WHEN no_data_found THEN
        fnd_file.put_line(fnd_file.log,
                          'Invalid Period: ' || p_period);
        RAISE e_invalid_period;
    END;
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step1:AUTOPOST_P Begin -----------------------');
    v_start_time := dbms_utility.get_time;
    posting(p_mrc_type => 'P');
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step1:AUTOPOST_P End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step2:AUTOPOST_R Begin-----------------------');
    v_start_time := dbms_utility.get_time;
    posting(p_mrc_type => 'R');
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step2:AUTOPOST_R End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step3:Remeasurement Begin -----------------------');
    v_start_time := dbms_utility.get_time;
    remeasure(p_period    => upper(p_period),
              p_rate_date => p_rate_date);
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step3:Remeasurement End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step4:AUTOPOST_P Begin -----------------------');
    v_start_time := dbms_utility.get_time;
    posting(p_mrc_type => 'P');
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step4:AUTOPOST_P End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step5:AUTOPOST_R Begin-----------------------');
    v_start_time := dbms_utility.get_time;
    posting(p_mrc_type => 'R');
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step5:AUTOPOST_R End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step6:Translation Begin -----------------------');
    v_start_time := dbms_utility.get_time;
    translate(p_period    => upper(p_period),
              p_rate_date => p_rate_date);
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step6:Translation End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step7:AUTOPOST_R Begin-----------------------');
    v_start_time := dbms_utility.get_time;
    posting(p_mrc_type => 'R');
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step7:AUTOPOST_R End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step8:US TAX Begin -----------------------');
    v_start_time := dbms_utility.get_time;
    ustax(p_period    => upper(p_period),
          p_rate_date => p_rate_date,
          p_us_year   => v_us_year,
          p_us_date   => v_us_date);
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step8:US TAX End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step9:AUTOPOST_P Begin -----------------------');
    v_start_time := dbms_utility.get_time;
    posting(p_mrc_type => 'P');
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step9:AUTOPOST_P End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step10:AUTOPOST_R Begin-----------------------');
    v_start_time := dbms_utility.get_time;
    posting(p_mrc_type => 'R');
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step10:AUTOPOST_R End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step11:Remeasurement Begin -----------------------');
    v_start_time := dbms_utility.get_time;
    remeasure(p_period    => upper(p_period),
              p_rate_date => p_rate_date);
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step11:Remeasurement End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step12:AUTOPOST_P Begin -----------------------');
    v_start_time := dbms_utility.get_time;
    posting(p_mrc_type => 'P');
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step12:AUTOPOST_P End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step13:AUTOPOST_R Begin-----------------------');
    v_start_time := dbms_utility.get_time;
    posting(p_mrc_type => 'R');
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step13:AUTOPOST_R End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step14:Translation Begin -----------------------');
    v_start_time := dbms_utility.get_time;
    translate(p_period    => upper(p_period),
              p_rate_date => p_rate_date);
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step14:Translation End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step15:AUTOPOST_R Begin-----------------------');
    v_start_time := dbms_utility.get_time;
    posting(p_mrc_type => 'R');
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Step15:AUTOPOST_R End. Elapsed Time:' ||
                      (dbms_utility.get_time - v_start_time) / 100 ||
                      '-----------------------');
  
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Status report begin.-----------------------');
    send_status_report(v_user_id,
                       'g00360508@mail.ad.ge.com',
                       v_stat,
                       v_msg);
    fnd_file.put_line(fnd_file.log,
                      '-----------------------Status report end.-----------------------');
  
  EXCEPTION
    WHEN e_invalid_period THEN
      retcode := 1;
    
    WHEN OTHERS THEN
      retcode := 2;
      errbuff := SQLERRM;
  END launchqueue;

  /* Send status mail */
  PROCEDURE send_status_report(p_user_id    IN NUMBER,
                               p_recipients IN VARCHAR2,
                               p_stat       OUT VARCHAR2,
                               p_msg        OUT VARCHAR2) IS
  
    -- Till here 
  
    v_stng      VARCHAR2(4000);
    v_step      INTEGER;
    v_step_name VARCHAR2(50);
    conn        utl_smtp.connection;
  
    v_serverinfo VARCHAR(200);
    v_requestor  VARCHAR(50);
    v_sender     VARCHAR2(100) := 'oracle_user@ge.com';
  
    v_startdate     VARCHAR(50);
    v_enddate       VARCHAR(50);
    v_totalduration VARCHAR(50);
  
  BEGIN
  
    fnd_file.put_line(fnd_file.log,
                      'Mail Sending...');
  
    SELECT upper(instance_name) || ' ON ' || host_name
      INTO v_serverinfo
      FROM v$instance;
  
    SELECT user_name
      INTO v_requestor
      FROM fnd_user
     WHERE user_id = p_user_id;
  
    SELECT to_char(actual_start_date,
                   'DD-MON-YYYY HH24:MI:SS'),
           to_char(nvl(actual_completion_date,
                       SYSDATE),
                   'DD-MON-YYYY HH24:MI:SS'),
           round((nvl(actual_completion_date,
                      SYSDATE) - actual_start_date) * 86400,
                 0)
      INTO v_startdate,
           v_enddate,
           v_totalduration
      FROM fnd_conc_req_summary_v
     WHERE request_id = v_conc_request
     ORDER BY request_id;
  
    conn := gerfp_ccl_mail.begin_mail(sender     => v_sender,
                                      recipients => p_recipients,
                                      subject    => 'Master Request Status Report',
                                      mime_type  => 'text/html');
  
    v_stng := '<html><head><meta http-equiv="Content-Type" content="text/html; charset=GBK"></head>' ||
              '<body><H1>Master Request Status Report </H1><BR>' ||
              'Server info      : ' || v_serverinfo || '<BR>' ||
              'Master Request ID: ' || v_conc_request || '<BR>' ||
              'Requestor        : ' || v_requestor || '<BR>' ||
              'Start Date       : ' || v_startdate || '<BR>' ||
              'End Date         : ' || v_enddate || '<BR>' ||
              'TotalDuration(s) : ' || v_totalduration || '</body></html>';
  
    gerfp_ccl_mail.write_text(conn    => conn,
                              message => v_stng);
  
    gerfp_ccl_mail.end_mail(conn => conn);
    fnd_file.put_line(fnd_file.log,
                      'Mail Send.');
    p_stat := 'S';
  
  EXCEPTION
    WHEN OTHERS THEN
      p_stat := 'E';
      fnd_file.put_line(fnd_file.log,
                        ' EXECEPTION IN PROCEDURE --> SEND_STATUS_REPORT' ||
                        SQLERRM);
  END send_status_report;

-- Enter further code below as specified in the Package spec.
END gerfp_monthend;
/