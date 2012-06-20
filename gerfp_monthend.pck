CREATE OR REPLACE PACKAGE GERFP_MONTHEND
  IS
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

   PROCEDURE LaunchQueue( errbuff   OUT  VARCHAR2
                   , retcode   OUT  VARCHAR2
                   , P_PERIOD IN VARCHAR2
                   );
                   
   PROCEDURE SEND_STATUS_REPORT(
                p_user_id IN number,
                P_RECIPIENTS    IN    VARCHAR2, 
                P_STAT         OUT    VARCHAR2,
                P_MSG          OUT    VARCHAR2); 
    
/* 
   FUNCTION function_name
     ( param1 IN datatype DEFAULT default_value, 
       param2 IN OUT datatype)
     RETURN  datatype;
*/
END; -- Package spec
/
CREATE OR REPLACE PACKAGE BODY GERFP_MONTHEND
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

v_conc_request        NUMBER      :=   FND_GLOBAL.CONC_REQUEST_ID;

   PROCEDURE LaunchQueue(errbuff   OUT  VARCHAR2
                   , retcode   OUT  VARCHAR2
                   , P_PERIOD IN VARCHAR2
                   )
    IS
      -- Enter the procedure variables here. As shown below
     --variable_name        datatype  NOT NULL DEFAULT default_value;
      V_User_id                 NUMBER;  
      V_REQ_ID           	    NUMBER;
      V_REQUEST_COMPLETE 		BOOLEAN;
      V_PHASE           	    VARCHAR2(20);
      V_STATUS           		VARCHAR2(20);
      V_DEV_PHASE        		VARCHAR2(20);
      V_DEV_STATUS       		VARCHAR2(20);
      V_MESSAGE          		VARCHAR2(200);
    
      V_Last_Req                NUMBER;              
      
      X_STATUS                  VARCHAR2(20);
      
      V_US_Year                    VARCHAR2(20);
      V_US_Date                    VARCHAR2(20);
      E_Invalid_Period             Exception; 
      
      V_STAT                       VARCHAR2(20);
      V_MSG                        VARCHAR2(20); 
      
      cursor C_Queue(p_user_id number,p_req_id number)
      is
      --select distinct REQUEST_ID from fnd_concurrent_requests 
      select REQUEST_ID,USER_CONCURRENT_PROGRAM_NAME, ARGUMENT_TEXT,REQUEST_DATE,REQUESTED_START_DATE,ACTUAL_START_DATE,ACTUAL_COMPLETION_DATE from FND_CONC_REQ_SUMMARY_V
      where 
        --nvl(PHASE_CODE,'N')<>'C' and 
        REQUESTED_BY=p_user_id and REQUEST_ID>p_req_id order by REQUEST_ID;
  
   BEGIN 
      X_STATUS                  := NULL;
      
      FND_GLOBAL.SET_NLS_CONTEXT (P_NLS_LANGUAGE => 'AMERICAN');  
      APPS.FND_PROFILE.GET('USER_ID', v_user_id);
      FND_FILE.PUT_LINE(FND_FILE.LOG,'The master request id is ' || v_conc_request);

--Validate the input parameter: period      
      Begin
          SELECT DISTINCT PERIOD_YEAR,'15-' || PERIOD_NAME INTO V_US_YEAR,V_US_DATE
          FROM GL_PERIOD_STATUSES WHERE APPLICATION_ID=101 AND CLOSING_STATUS IN ('O') AND PERIOD_TYPE='21' 
          AND ADJUSTMENT_PERIOD_FLAG<>'Y' AND PERIOD_NAME=upper(P_PERIOD);
      
      EXCEPTION
          WHEN NO_DATA_FOUND THEN 
            FND_FILE.PUT_LINE( FND_FILE.LOG,'Invalid Period: ' || P_PERIOD);
            Raise E_Invalid_Period;
      END;
       
--P book Post
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Primary Book Post begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOPOSTING'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'P'
				   ,ARGUMENT2     => Null
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step1:AUTOPOST_P | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        
        LOOP

            V_Last_Req:=V_REQ_ID;        
            
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;
                    
            dbms_lock.sleep(60);
            
            V_REQ_ID :=null;
            
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
                LOOP
                  V_PHASE      				:= NULL;
                  V_STATUS     				:= NULL;
                  V_DEV_PHASE  				:= NULL;
                  V_DEV_STATUS 				:= NULL;
                  V_MESSAGE    				:= NULL;
     
                  
                  V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step1:AUTOPOST_P | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
                        EXIT;
                    END IF;
                END LOOP;
            END LOOP;        
        END LOOP;
        
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Primary Book Post end.---');

--R book Post
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Post begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOPOSTING'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'R'
				   ,ARGUMENT2     => Null
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step1:AUTOPOST_R | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        
        LOOP

            V_Last_Req:=V_REQ_ID;        
            
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;
                    
            dbms_lock.sleep(60);
            
            V_REQ_ID :=null;
            
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
    
                LOOP
                    V_PHASE      				:= NULL;
                    V_STATUS     				:= NULL;
                    V_DEV_PHASE  				:= NULL;
                    V_DEV_STATUS 				:= NULL;
                    V_MESSAGE    				:= NULL;
                    
                    
                    V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
                    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step1:AUTOPOST_R | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP; 
        END LOOP;    
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Post end.---');


--P book Revaluation
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Primary Book Revaluation begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOREVAL'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'P'
				   ,ARGUMENT2     => Upper(P_PERIOD)
				   ,ARGUMENT3     => NULL
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step2:AUTOREVAL_P | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
    
                LOOP
                    V_PHASE      				:= NULL;
                    V_STATUS     				:= NULL;
                    V_DEV_PHASE  				:= NULL;
                    V_DEV_STATUS 				:= NULL;
                    V_MESSAGE    				:= NULL;
                    
                    
                    V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
                    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step2:AUTOREVAL_P | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));                        
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP; 
        END LOOP;    
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Primary Book Revaluation end.---');

--R book Revaluation
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Revaluation begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOREVAL'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'R'
				   ,ARGUMENT2     => Upper(P_PERIOD)
				   ,ARGUMENT3     => NULL
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step2:AUTOREVAL_R | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
    
                LOOP
                    V_PHASE      				:= NULL;
                    V_STATUS     				:= NULL;
                    V_DEV_PHASE  				:= NULL;
                    V_DEV_STATUS 				:= NULL;
                    V_MESSAGE    				:= NULL;
                    
                    
                    V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
                    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step2:AUTOREVAL_R | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));                        
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP;
        END LOOP;     
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Revaluation end.---');

--P book Post
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Primary Book Post begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOPOSTING'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'P'
				   ,ARGUMENT2     => Null
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step3:AUTOPOST_P | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
                LOOP
                  V_PHASE      				:= NULL;
                  V_STATUS     				:= NULL;
                  V_DEV_PHASE  				:= NULL;
                  V_DEV_STATUS 				:= NULL;
                  V_MESSAGE    				:= NULL;
     
                  
                  V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step3:AUTOPOST_P | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));                        
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP;
        END LOOP;     
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Primary Book Post end.---');

--R book Post
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Post begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOPOSTING'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'R'
				   ,ARGUMENT2     => Null
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step3:AUTOPOST_R | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
    
                LOOP
                    V_PHASE      				:= NULL;
                    V_STATUS     				:= NULL;
                    V_DEV_PHASE  				:= NULL;
                    V_DEV_STATUS 				:= NULL;
                    V_MESSAGE    				:= NULL;
                    
                    
                    V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
                    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step3:AUTOPOST_R | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP; 
        END LOOP;    
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Post end.---');


--P book Revaluation
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Primary Book Revaluation begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOREVAL'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'P'
				   ,ARGUMENT2     => Upper(P_PERIOD)
				   ,ARGUMENT3     => NULL
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step4:AUTOREVAL_P | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
    
                LOOP
                    V_PHASE      				:= NULL;
                    V_STATUS     				:= NULL;
                    V_DEV_PHASE  				:= NULL;
                    V_DEV_STATUS 				:= NULL;
                    V_MESSAGE    				:= NULL;
                    
                    
                    V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
                    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step4:AUTOREVAL_P | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP;
        END LOOP;     
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Primary Book Revaluation end.---');

--R book Revaluation
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Revaluation begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOREVAL'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'R'
				   ,ARGUMENT2     => Upper(P_PERIOD)
				   ,ARGUMENT3     => NULL
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step4:AUTOREVAL_R | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
    
                LOOP
                    V_PHASE      				:= NULL;
                    V_STATUS     				:= NULL;
                    V_DEV_PHASE  				:= NULL;
                    V_DEV_STATUS 				:= NULL;
                    V_MESSAGE    				:= NULL;
                    
                    
                    V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
                    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step4:AUTOREVAL_R | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));                        
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP;
        END LOOP;     
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Revaluation end.---');

--US tax
FND_FILE.PUT_LINE(FND_FILE.LOG,'---US tax begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_AUTO_RUN_US_TAX'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => V_US_YEAR
				   ,ARGUMENT2     => upper(P_PERIOD)
				   ,ARGUMENT3     => V_US_Date
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step5:US_TAX | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
    
                LOOP
                    V_PHASE      				:= NULL;
                    V_STATUS     				:= NULL;
                    V_DEV_PHASE  				:= NULL;
                    V_DEV_STATUS 				:= NULL;
                    V_MESSAGE    				:= NULL;
                    
                    
                    V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
                    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step5:US_TAX | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));                                                
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP; 
        END LOOP;    
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---US tax end.---');

--P book Post
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Primary Book Post begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOPOSTING'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'P'
				   ,ARGUMENT2     => Null
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step6:AUTOPOST_P | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));

        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
                LOOP
                  V_PHASE      				:= NULL;
                  V_STATUS     				:= NULL;
                  V_DEV_PHASE  				:= NULL;
                  V_DEV_STATUS 				:= NULL;
                  V_MESSAGE    				:= NULL;
     
                  
                  V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step6:AUTOPOST_P | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP;
        END LOOP;     
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Primary Book Post end.---');

--R book Post
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Post begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOPOSTING'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'R'
				   ,ARGUMENT2     => Null
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step6:AUTOPOST_R | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
    
                LOOP
                    V_PHASE      				:= NULL;
                    V_STATUS     				:= NULL;
                    V_DEV_PHASE  				:= NULL;
                    V_DEV_STATUS 				:= NULL;
                    V_MESSAGE    				:= NULL;
                    
                    
                    V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
                    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step6:AUTOPOST_R | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));                        
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP; 
        END LOOP;    
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Post end.---');

--P book Revaluation
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Primary Book Revaluation begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOREVAL'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'P'
				   ,ARGUMENT2     => upper(P_PERIOD)
				   ,ARGUMENT3     => NULL
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step7:AUTOREVAL_P | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
    
                LOOP
                    V_PHASE      				:= NULL;
                    V_STATUS     				:= NULL;
                    V_DEV_PHASE  				:= NULL;
                    V_DEV_STATUS 				:= NULL;
                    V_MESSAGE    				:= NULL;
                    
                    
                    V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
                    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step7:AUTOREVAL_P | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));                                                
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP; 
        END LOOP;    
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Primary Book Revaluation end.---');

--R book Revaluation
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Revaluation begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOREVAL'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'R'
				   ,ARGUMENT2     => upper(P_PERIOD)
				   ,ARGUMENT3     => NULL
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step7:AUTOREVAL_R | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
    
                LOOP
                    V_PHASE      				:= NULL;
                    V_STATUS     				:= NULL;
                    V_DEV_PHASE  				:= NULL;
                    V_DEV_STATUS 				:= NULL;
                    V_MESSAGE    				:= NULL;
                    
                    
                    V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
                    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step7:AUTOREVAL_R | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));                                                                        
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP;
        END LOOP;     
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Revaluation end.---');

--GERFP Auto Run 1/1 Revaluation
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Jan1 Revaluation begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_AUTO_RUN_PY_REVALUE'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => upper(P_PERIOD)
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step8:AUTOREVAL_JAN1 | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
    
                LOOP
                    V_PHASE      				:= NULL;
                    V_STATUS     				:= NULL;
                    V_DEV_PHASE  				:= NULL;
                    V_DEV_STATUS 				:= NULL;
                    V_MESSAGE    				:= NULL;
                    
                    
                    V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
                    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step8:AUTOREVAL_JAN1 | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));                                                                        
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP;
        END LOOP;     
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Jan1 Revaluation end.---');

--R book Post
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Post begin.---');
    V_REQ_ID := FND_REQUEST.SUBMIT_REQUEST( APPLICATION   => 'SQLGL'
				   ,PROGRAM       => 'GERFP_GL_AUTOPOSTING'
				   ,DESCRIPTION   => NULL
				   ,START_TIME    => SYSDATE
				   ,SUB_REQUEST   => FALSE
				   ,ARGUMENT1     => 'R'
				   ,ARGUMENT2     => Null
				   );
    Commit;	
    IF V_REQ_ID=0 THEN
        RAISE_APPLICATION_ERROR(-20160, FND_MESSAGE.GET);
        X_STATUS := 'FAILED';
    ELSE
        X_STATUS := 'DONE';
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step9:AUTOPOST_R | ' || V_REQ_ID || '|' || X_STATUS || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
        
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check begins.');
        LOOP
            V_Last_Req:=V_REQ_ID;        
            IF NVL(V_Last_Req,0)=0 THEN 
                EXIT;
            END IF;

            dbms_lock.sleep(60);
            V_REQ_ID :=null;
            FOR V_REQUEST IN C_QUEUE(V_USER_ID,V_Last_Req)
            LOOP
                
                V_REQ_ID := V_REQUEST.REQUEST_ID;
    
                LOOP
                    V_PHASE      				:= NULL;
                    V_STATUS     				:= NULL;
                    V_DEV_PHASE  				:= NULL;
                    V_DEV_STATUS 				:= NULL;
                    V_MESSAGE    				:= NULL;
                    
                    
                    V_REQUEST_COMPLETE := APPS.FND_CONCURRENT.WAIT_FOR_REQUEST(V_REQ_ID,
                                                                  	    10,
                                                                        9999,
                                                                    	V_PHASE,
                                                                    	V_STATUS,
                                                                    	V_DEV_PHASE,
                                                                    	V_DEV_STATUS,
                                                                    	V_MESSAGE);
                    
                    IF UPPER(V_PHASE) = 'COMPLETED' THEN
                        FND_FILE.PUT_LINE(FND_FILE.LOG,'Step9:AUTOPOST_R | ' || V_REQ_ID || '|' || V_REQUEST.USER_CONCURRENT_PROGRAM_NAME || '(' || V_REQUEST.ARGUMENT_TEXT || ')|' || V_PHASE || '|' || V_STATUS || '|' || V_REQUEST.REQUEST_DATE || '|' || V_REQUEST.REQUESTED_START_DATE || '|' || V_REQUEST.ACTUAL_START_DATE || '|' || V_REQUEST.ACTUAL_COMPLETION_DATE || '|' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));                        
                        EXIT;
                    END IF;
                END LOOP; 
            END LOOP; 
        END LOOP;    
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Quene check ends.');
    END IF;    
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Reporting Book Post end.---');

FND_FILE.PUT_LINE(FND_FILE.LOG,'---Status report begin.---');
--Status Report: GERFP_CCL_EMAIL/ CorpGBSRFPCCLORASIAinterfacealerts@ge.com
    SEND_STATUS_REPORT(v_user_id,'CorpGBSRFPCCLORASIAinterfacealerts@ge.com',V_STAT,V_MSG);
FND_FILE.PUT_LINE(FND_FILE.LOG,'---Status report end.---');
               
     --  statements ;
   EXCEPTION
      when E_Invalid_Period then 
          retcode:=1;
          
      WHEN others THEN
          retcode:=2; 
          errbuff:=SQLERRM; 
   END LaunchQueue;

/* Send status mail */
    PROCEDURE SEND_STATUS_REPORT(
                    p_user_id IN number,
                    P_RECIPIENTS    IN    VARCHAR2, 
                    P_STAT         OUT    VARCHAR2,
                    P_MSG          OUT    VARCHAR2)
    IS

-- Till here 
    
    v_stng        VARCHAR2(4000);
    v_step        integer;
    v_step_name   varchar2(50);    
    conn utl_smtp.connection;
    
    v_serverinfo varchar(200);
    v_requestor Varchar(50);
    v_sender    VARCHAR2(100) := 'oracle_user@ge.com';
    
    V_StartDate varchar(50);
    V_EndDate varchar(50);
    V_TotalDuration varchar(50);   
    
    BEGIN
    
    FND_FILE.PUT_LINE(FND_FILE.LOG,'Mail Sending...'); 
    
    SELECT UPPER(INSTANCE_NAME) || ' ON ' || HOST_NAME INTO V_SERVERINFO FROM V$INSTANCE;
    
    SELECT USER_NAME INTO V_REQUESTOR FROM FND_USER WHERE USER_ID=P_USER_ID; 
    
    SELECT TO_CHAR(ACTUAL_START_DATE,'DD-MON-YYYY HH24:MI:SS'),
      TO_CHAR(NVL(ACTUAL_COMPLETION_DATE,SYSDATE),'DD-MON-YYYY HH24:MI:SS'),
      ROUND((NVL(ACTUAL_COMPLETION_DATE,SYSDATE)-ACTUAL_START_DATE)* 86400,0) 
      into V_STARTDATE,V_ENDDATE,V_TotalDuration
      FROM FND_CONC_REQ_SUMMARY_V
      WHERE REQUEST_ID=V_CONC_REQUEST ORDER BY REQUEST_ID;

    conn := GERFP_CCL_MAIL.begin_mail(sender     => v_sender,
                     recipients => p_recipients,
                     subject    => 'Master Request Status Report',
                     mime_type  => 'text/html');

    v_stng := '<html><head><meta http-equiv="Content-Type" content="text/html; charset=GBK"></head>'
          ||'<body><H1>Master Request Status Report </H1><BR>' 
          || 'Server info      : ' || V_SERVERINFO || '<BR>'
          || 'Master Request ID: ' || v_conc_request || '<BR>'
          || 'Requestor        : ' || v_requestor || '<BR>'     
          || 'Start Date       : ' || V_StartDate || '<BR>'
          || 'End Date         : ' || V_EndDate || '<BR>'
          || 'TotalDuration(s) : ' || V_TotalDuration || '</body></html>';
          

    GERFP_CCL_MAIL.write_text(conn    => conn,
                 message => v_stng);
 
    GERFP_CCL_MAIL.end_mail( conn => conn );
    FND_FILE.PUT_LINE(FND_FILE.LOG,'Mail Send.');  
    p_stat :='S';

    EXCEPTION
     WHEN OTHERS THEN
        P_STAT := 'E';
        FND_FILE.PUT_LINE(FND_FILE.LOG,' EXECEPTION IN PROCEDURE --> SEND_STATUS_REPORT'||SQLERRM);
    END SEND_STATUS_REPORT;


   -- Enter further code below as specified in the Package spec.
END GERFP_MONTHEND;
/
