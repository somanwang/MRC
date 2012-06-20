CREATE OR REPLACE PACKAGE gerfp_utl IS

  -- Author  : HW70001208
  -- Created : 9/8/2011 3:36:42 PM
  -- Purpose : 
  ----Returns The Number Of Seconds Between Two Date-Time Values
  FUNCTION seconds_diff(date_1 IN DATE,
                        date_2 IN DATE) RETURN NUMBER;

  --mail the output of a specified request as an attachement
  PROCEDURE mail_request_output(p_request_id    IN NUMBER,
                                p_recipients    IN VARCHAR2, -- seperate by comma
                                p_subject       IN VARCHAR2,
                                x_return_status OUT VARCHAR2,
                                x_error_msg     OUT VARCHAR2);

  PROCEDURE debug(p_msg IN VARCHAR2);
  PROCEDURE output(p_msg IN VARCHAR2);
  FUNCTION convert_value(p_original_value IN VARCHAR2) RETURN VARCHAR2;
  FUNCTION convert_value(p_original_value IN DATE) RETURN VARCHAR2;
  FUNCTION convert_value4web(p_original_value IN VARCHAR2) RETURN VARCHAR2;
  FUNCTION convert_value4web(p_original_value IN NUMBER) RETURN VARCHAR2;
  FUNCTION convert_value4web(p_original_value IN DATE) RETURN VARCHAR2;
  /*begin
      showlong( 'select text from all_views where view_name = :x', ':x', 'ALL_VIEWS' );
  end;*/
  PROCEDURE showlong(p_query IN VARCHAR2,
                     p_name  IN VARCHAR2,
                     p_value IN VARCHAR2);

  --- sob_id =-1, not exist
  PROCEDURE get_sob_id(p_me_code   IN VARCHAR2,
                       p_le_code   IN VARCHAR2,
                       p_book_type IN VARCHAR2,
                       x_p_sob_id  OUT NUMBER,
                       x_r_sob_id  OUT NUMBER);

  PROCEDURE reverse_je(p_je_header_id IN NUMBER,
                       p_rev_period   IN VARCHAR2,
                       x_message      OUT VARCHAR2,
                       x_status       OUT VARCHAR2);

END gerfp_utl;
/
CREATE OR REPLACE PACKAGE BODY gerfp_utl IS

  PROCEDURE debug(p_msg IN VARCHAR2) IS
  BEGIN
    --dbms_output.put_line(p_msg);
    fnd_file.put_line(fnd_file.log,
                      p_msg);
  END;

  PROCEDURE output(p_msg IN VARCHAR2) IS
  BEGIN
    --dbms_output.put_line(p_msg);
    fnd_file.put_line(fnd_file.output,
                      p_msg);
  END;

  FUNCTION convert_value(p_original_value IN VARCHAR2) RETURN VARCHAR2 IS
  
    l_value VARCHAR2(32767);
  BEGIN
  
    l_value := p_original_value;
    l_value := htf.escape_url(l_value);
    RETURN l_value;
  END;

  FUNCTION convert_value(p_original_value IN DATE) RETURN VARCHAR2 IS
  
  BEGIN
  
    RETURN convert_value(to_char(p_original_value,
                                 'dd-mon-yyyy'));
  END;

  FUNCTION convert_value4web(p_original_value IN VARCHAR2) RETURN VARCHAR2 IS
  
  BEGIN
    RETURN convert_value('="' || p_original_value || '"');
  END;

  FUNCTION convert_value4web(p_original_value IN NUMBER) RETURN VARCHAR2 IS
  
  BEGIN
    RETURN convert_value(p_original_value);
  END;

  FUNCTION convert_value4web(p_original_value IN DATE) RETURN VARCHAR2 IS
  
  BEGIN
    RETURN convert_value(p_original_value);
  END;

  ----Returns The Number Of Seconds Between Two Date-Time Values
  FUNCTION seconds_diff(date_1 IN DATE,
                        date_2 IN DATE) RETURN NUMBER IS
  
    ndate_1   NUMBER;
    ndate_2   NUMBER;
    nsecond_1 NUMBER(5,
                     0);
    nsecond_2 NUMBER(5,
                     0);
  
  BEGIN
    -- Get Julian date number from first date (DATE_1)
    ndate_1 := to_number(to_char(date_1,
                                 'J'));
  
    -- Get Julian date number from second date (DATE_2)
    ndate_2 := to_number(to_char(date_2,
                                 'J'));
  
    -- Get seconds since midnight from first date (DATE_1)
    nsecond_1 := to_number(to_char(date_1,
                                   'SSSSS'));
  
    -- Get seconds since midnight from second date (DATE_2)
    nsecond_2 := to_number(to_char(date_2,
                                   'SSSSS'));
  
    RETURN(((ndate_2 - ndate_1) * 86400) + (nsecond_2 - nsecond_1));
  END;

  --mail the output of a specified request as an attachement
  PROCEDURE mail_request_output(p_request_id    IN NUMBER,
                                p_recipients    IN VARCHAR2, -- seperate by comma
                                p_subject       IN VARCHAR2,
                                x_return_status OUT VARCHAR2,
                                x_error_msg     OUT VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  
    l_request_id NUMBER;
  
    l_phase         VARCHAR2(2000) DEFAULT NULL; -- phase
    l_status        VARCHAR2(2000) DEFAULT NULL; -- status
    l_dev_phase     VARCHAR2(2000) DEFAULT NULL; -- dev phase
    l_dev_status    VARCHAR2(2000) DEFAULT NULL; -- dev status
    l_return_status BOOLEAN DEFAULT TRUE; -- return status
    l_msg_data      VARCHAR2(2000) DEFAULT NULL;
    l_subject       VARCHAR2(2000);
  
  BEGIN
  
    debug('--------------entering gerfp_utl.mail_request_output-----------------------');
    l_subject := REPLACE(p_subject,
                         ' ',
                         '_');
  
    l_request_id := fnd_request.submit_request(application => 'XXRFP',
                                               program     => 'GERFPMRO',
                                               description => NULL,
                                               start_time  => NULL,
                                               argument1   => p_request_id,
                                               argument2   => p_recipients,
                                               argument3   => l_subject,
                                               argument4   => chr(0));
  
    IF (l_request_id = 0) THEN
      debug('Failed to Submit import program. Error msg: ' ||
            fnd_message.get);
      RAISE fnd_api.g_exc_error;
    
    ELSE
    
      debug('Submit import program. Reqeust id :' || l_request_id);
      COMMIT;
    
      /*l_return_status := fnd_concurrent.wait_for_request(request_id => l_request_id,
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
        
          debug('Request failed. Status:' || l_status || ' Phase:' ||
                l_phase);
          RAISE fnd_api.g_exc_error;
        
        END IF;
      
      ELSE
      
        debug('WAIT FOR REQUEST FAILED - STATUS UNKNOWN');
        RAISE fnd_api.g_exc_error;
      
      END IF;*/
    
    END IF;
  
    x_return_status := fnd_api.g_ret_sts_success;
  
  EXCEPTION
    WHEN OTHERS THEN
      x_return_status := fnd_api.g_ret_sts_error;
      x_error_msg     := SQLERRM;
    
  END;

  /*begin
      showlong( 'select text from all_views where view_name = :x', ':x', 'ALL_VIEWS' );
  end;*/
  PROCEDURE showlong(p_query IN VARCHAR2,
                     p_name  IN VARCHAR2,
                     p_value IN VARCHAR2) AS
    l_cursor   INTEGER DEFAULT dbms_sql.open_cursor;
    l_n        NUMBER;
    l_long_val VARCHAR2(250);
    l_long_len NUMBER;
    l_buflen   NUMBER := 250;
    l_curpos   NUMBER := 0;
  BEGIN
    dbms_sql.parse(l_cursor,
                   p_query,
                   dbms_sql.native);
    dbms_sql.bind_variable(l_cursor,
                           p_name,
                           p_value);
  
    dbms_sql.define_column_long(l_cursor,
                                1);
    l_n := dbms_sql.execute(l_cursor);
  
    IF (dbms_sql.fetch_rows(l_cursor) > 0) THEN
      LOOP
        dbms_sql.column_value_long(l_cursor,
                                   1,
                                   l_buflen,
                                   l_curpos,
                                   l_long_val,
                                   l_long_len);
        l_curpos := l_curpos + l_long_len;
        dbms_output.put_line(l_long_val);
        EXIT WHEN l_long_len = 0;
      END LOOP;
    END IF;
    dbms_output.put_line('====================');
    dbms_output.put_line('Long was ' || l_curpos || ' bytes in length');
    dbms_sql.close_cursor(l_cursor);
  EXCEPTION
    WHEN OTHERS THEN
      IF dbms_sql.is_open(l_cursor) THEN
        dbms_sql.close_cursor(l_cursor);
      END IF;
      RAISE;
  END showlong;

  PROCEDURE get_sob_id(p_me_code   IN VARCHAR2,
                       p_le_code   IN VARCHAR2,
                       p_book_type IN VARCHAR2,
                       x_p_sob_id  OUT NUMBER,
                       x_r_sob_id  OUT NUMBER) IS
  
    l_type_code VARCHAR2(100);
    l_sob_id    NUMBER;
  
  BEGIN
  
    SELECT sob.mrc_sob_type_code,
           sob.set_of_books_id
      INTO l_type_code,
           l_sob_id
      FROM gl_sets_of_books      sob,
           xxrfp_shelton_bus_map bus
     WHERE sob.name = bus.sob_name
       AND bus.me_code = p_me_code
       AND bus.le_code = p_le_code
       AND bus.book_type = p_book_type
       AND bus.le_code != '000000'
       AND bus.primary_flag = 'Y';
  
    IF l_type_code = 'N' THEN
    
      x_p_sob_id := l_sob_id;
      x_r_sob_id := -1;
    
    ELSIF l_type_code = 'P' THEN
      x_p_sob_id := l_sob_id;
    
      BEGIN
        SELECT ba.primary_set_of_books_id,
               ba.reporting_set_of_books_id
          INTO x_p_sob_id,
               x_r_sob_id
          FROM gl_mc_book_assignments ba
         WHERE decode(l_type_code,
                      'P',
                      ba.primary_set_of_books_id,
                      ba.reporting_set_of_books_id) = l_sob_id;
      EXCEPTION
        WHEN no_data_found THEN
          x_r_sob_id := -1;
      END;
    
    ELSIF l_type_code = 'R' THEN
    
      x_r_sob_id := l_sob_id;
    
      BEGIN
        SELECT ba.primary_set_of_books_id,
               ba.reporting_set_of_books_id
          INTO x_p_sob_id,
               x_r_sob_id
          FROM gl_mc_book_assignments ba
         WHERE decode(l_type_code,
                      'P',
                      ba.primary_set_of_books_id,
                      ba.reporting_set_of_books_id) = l_sob_id;
      EXCEPTION
        WHEN no_data_found THEN
          x_p_sob_id := -1;
      END;
    
    END IF;
  
  END;

  PROCEDURE reverse_je(p_je_header_id IN NUMBER,
                       p_rev_period   IN VARCHAR2,
                       x_message      OUT VARCHAR2,
                       x_status       OUT VARCHAR2) IS
  
    l_req_id             NUMBER;
    l_accrual_rev_status VARCHAR2(100);
    PRAGMA AUTONOMOUS_TRANSACTION;
  
  BEGIN
  
    x_status := 'S';
  
    dbms_output.put_line('Reversing JE header id = ' || p_je_header_id);
  
    fnd_global.apps_initialize(user_id      => 1550,
                               resp_id      => 50303,
                               resp_appl_id => 101);
  
    SELECT t.accrual_rev_status
      INTO l_accrual_rev_status
      FROM gl_je_headers t
     WHERE t.je_header_id = p_je_header_id;
  
    IF l_accrual_rev_status IS NOT NULL THEN
      x_message := ' reverse status is wrong:' || l_accrual_rev_status;
      dbms_output.put_line(x_message);
      x_status := 'W';
      ROLLBACK;
      RETURN;
    END IF;
  
    UPDATE gl_je_headers t
       SET t.accrual_rev_flag        = 'Y',
           t.accrual_rev_period_name = p_rev_period
     WHERE t.je_header_id = p_je_header_id;
  
    l_req_id := fnd_request.submit_request(application => 'SQLGL',
                                           program     => 'GLPREV',
                                           description => NULL,
                                           start_time  => SYSDATE,
                                           sub_request => FALSE,
                                           argument1   => p_je_header_id);
  
    COMMIT;
  
    dbms_output.put_line(' Request id :' || l_req_id);
  
  EXCEPTION
    WHEN OTHERS THEN
    
      x_message := 'Errors in procedure reverse_je. Errors:' || SQLERRM;
      dbms_output.put_line(x_message);
      x_status := 'E';
    
  END;

END gerfp_utl;
/
