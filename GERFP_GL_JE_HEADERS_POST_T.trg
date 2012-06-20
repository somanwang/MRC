CREATE OR REPLACE TRIGGER GERFP_GL_JE_HEADERS_POST_T
 AFTER 
 UPDATE OF POSTED_DATE
 ON GL.GL_JE_HEADERS
 REFERENCING OLD AS OLD NEW AS NEW
 FOR EACH ROW 
when (old.posted_date is null  and new.status='P')
DECLARE
  reqid                   NUMBER;
  retval                  BOOLEAN;
  v_mrc_book_type         VARCHAR2(1);
  v_user_je_source_name   VARCHAR2(100);
  v_user_je_category_name VARCHAR2(100);
  l_fc                    VARCHAR2(100);

BEGIN

  retval := fnd_request.set_mode(db_trigger => TRUE);
  retval := fnd_request.set_options(implicit => 'NO');
  BEGIN
    SELECT mrc_sob_type_code
      INTO v_mrc_book_type
      FROM apps.gl_sets_of_books
     WHERE set_of_books_id = :old.set_of_books_id; --p_sob_id;
  EXCEPTION
    WHEN OTHERS THEN
      raise_application_error(-20160,
                              'Errors in geting mrc book type from gl_sets_of_books. sob_id=' ||
                              :old.set_of_books_id || ' Errors: ' ||
                              SQLERRM);
  END;

  IF nvl(v_mrc_book_type,
         'P') <> 'R' THEN
  
    BEGIN
    
      SELECT user_je_source_name
        INTO v_user_je_source_name
        FROM gl_je_sources_vl
       WHERE je_source_name = :old.je_source;
    
    EXCEPTION
      WHEN OTHERS THEN
        raise_application_error(-20160,
                                'Errors in getting user source name from gl_je_sources. JE Source=' ||
                                :old.je_source || ' Errors: ' || SQLERRM);
    END;
  
    IF v_user_je_source_name = 'Remeasurement' THEN
    
      BEGIN
      
        SELECT t.user_je_category_name
          INTO v_user_je_category_name
          FROM gl_je_categories t
         WHERE t.je_category_name = :old.je_category;
      
      EXCEPTION
        WHEN OTHERS THEN
          raise_application_error(-20160,
                                  'Errors in getting user category name from gl_je_categories. JE Category=' ||
                                  :old.je_category || ' Errors: ' ||
                                  SQLERRM);
      END;
    
      IF v_user_je_category_name = 'Remeasurement' THEN
      
        BEGIN
        
          SELECT bus.export4
            INTO l_fc
            FROM xxrfp_shelton_bus_map bus,
                 gl_je_lines           glj,
                 gl_code_combinations  gcc
           WHERE bus.primary_flag = 'Y'
             AND bus.le_code != '000000'
             AND bus.me_code = gcc.segment1
             AND bus.le_code = gcc.segment2
             AND bus.book_type = gcc.segment3
             AND glj.je_header_id = :old.je_header_id
             AND glj.code_combination_id = gcc.code_combination_id
             AND rownum = 1;
        
        EXCEPTION
          WHEN OTHERS THEN
            raise_application_error(-20160,
                                    'Errors in getting functional currency from xxrfp_shelton_bus_map. Errors: ' ||
                                    SQLERRM);
        END;
      
        IF l_fc = 'LC' THEN
        
          /* Only PSOB Revaluation Journal should process */
          reqid := fnd_request.submit_request('XXRFP',
                                              'GERFPGRPR',
                                              NULL,
                                              to_char(SYSDATE,
                                                      'DD-MON-RR HH24:MI:SS'),
                                              FALSE,
                                              :old.je_header_id,
                                              :old.period_name,
                                              :old.set_of_books_id,
                                              NULL);
        
          IF reqid = 0 THEN
            raise_application_error(-20160,
                                    fnd_message.get);
          END IF;
        
        END IF; --l_fc = 'LC'
      
      END IF; --v_user_je_category_name = 'Remeasurement'
    
    END IF; --IF v_user_je_source_name = 'Remeasurement'
  
  END IF; --nvl(v_mrc_book_type,'P') <> 'R'

END;
/
