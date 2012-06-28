CREATE OR REPLACE PACKAGE hw_api AS
  /*==================================================
  
  Description:
      This program provide common API for procedure
  History:
      1.00  2006-06-01  jim.lin    Creation
      1.01  2006-10-19  jim.lin    8.1.7 varray不支持boolean类型，且不能在spec进行初始化
  ==================================================*/

  -- Global Constants
  g_fnd_app  CONSTANT VARCHAR2(200) := 'FND';
  g_app_name CONSTANT VARCHAR2(200) := 'HSS';
  g_app_id NUMBER;

  -- execute exception type
  g_exc_name_error  CONSTANT VARCHAR2(30) := 'ERROR';
  g_exc_name_unexp  CONSTANT VARCHAR2(30) := 'UNEXP';
  g_exc_name_others CONSTANT VARCHAR2(30) := 'OTHERS';

  -- table action type
  g_act_create CONSTANT VARCHAR2(30) := 'CREATE';
  g_act_update CONSTANT VARCHAR2(30) := 'UPDATE';
  g_act_delete CONSTANT VARCHAR2(30) := 'DELETE';

  -- table event type
  TYPE event_enable_type IS VARRAY(6) OF VARCHAR2(1); -- BOOLEAN; 8.1.7不支持
  init_event_enable event_enable_type; -- := EVENT_ENABLE_TYPE('F','F','F','F','F','F');

  g_evt_pre_insert  CONSTANT PLS_INTEGER := 1; -- 'PRE_INSERT';
  g_evt_post_insert CONSTANT PLS_INTEGER := 2; -- 'POST_INSERT';
  g_evt_pre_update  CONSTANT PLS_INTEGER := 3; -- 'PRE_UPDATE';
  g_evt_post_update CONSTANT PLS_INTEGER := 4; -- 'POST_UPDATE';
  g_evt_pre_delete  CONSTANT PLS_INTEGER := 5; -- 'PRE_DELETE';
  g_evt_post_delete CONSTANT PLS_INTEGER := 6; -- 'POST_DELETE';

  g_ret_sts_success     CONSTANT VARCHAR2(1) := 'S';
  g_ret_sts_error       CONSTANT VARCHAR2(1) := 'E';
  g_ret_sts_unexp_error CONSTANT VARCHAR2(1) := 'U';
  g_ret_sts_warning     CONSTANT VARCHAR2(1) := 'W';

  g_exc_error EXCEPTION;
  g_exc_unexpected_error EXCEPTION;

  g_true  CONSTANT VARCHAR2(1) := 'T';
  g_false CONSTANT VARCHAR2(1) := 'F';

  PROCEDURE stack_message(p_msg VARCHAR2);
  PROCEDURE raise_exception(p_return_status     VARCHAR2,
                            px_procedure_status IN OUT VARCHAR2);

  PROCEDURE init_msg_list(p_init_msg_list IN VARCHAR2);

  FUNCTION start_activity(p_pkg_name       IN VARCHAR2,
                          p_api_name       IN VARCHAR2,
                          p_savepoint_name IN VARCHAR2 DEFAULT NULL,
                          p_init_msg_list  IN VARCHAR2 DEFAULT fnd_api.g_false,
                          l_api_version    IN NUMBER DEFAULT NULL,
                          p_api_version    IN NUMBER DEFAULT NULL)
    RETURN VARCHAR2;

  FUNCTION end_activity(p_pkg_name         IN VARCHAR2,
                        p_api_name         IN VARCHAR2,
                        p_commit           IN VARCHAR2 DEFAULT NULL,
                        p_procedure_status IN VARCHAR2,
                        x_msg_count        OUT NOCOPY NUMBER,
                        x_msg_data         OUT NOCOPY VARCHAR2)
    RETURN VARCHAR2;

  FUNCTION handle_exceptions(p_pkg_name       IN VARCHAR2,
                             p_api_name       IN VARCHAR2,
                             p_savepoint_name IN VARCHAR2 DEFAULT NULL,
                             p_exc_name       IN VARCHAR2,
                             x_msg_count      OUT NOCOPY NUMBER,
                             x_msg_data       OUT NOCOPY VARCHAR2)
    RETURN VARCHAR2;

  PROCEDURE set_message(p_app_name     IN VARCHAR2 DEFAULT hw_api.g_app_name,
                        p_msg_name     IN VARCHAR2,
                        p_token1       IN VARCHAR2 DEFAULT NULL,
                        p_token1_value IN VARCHAR2 DEFAULT NULL,
                        p_token2       IN VARCHAR2 DEFAULT NULL,
                        p_token2_value IN VARCHAR2 DEFAULT NULL,
                        p_token3       IN VARCHAR2 DEFAULT NULL,
                        p_token3_value IN VARCHAR2 DEFAULT NULL,
                        p_token4       IN VARCHAR2 DEFAULT NULL,
                        p_token4_value IN VARCHAR2 DEFAULT NULL,
                        p_token5       IN VARCHAR2 DEFAULT NULL,
                        p_token5_value IN VARCHAR2 DEFAULT NULL);

  PROCEDURE raise_exception(p_app_name     IN VARCHAR2 DEFAULT hw_api.g_app_name,
                            p_msg_name     IN VARCHAR2 DEFAULT NULL,
                            p_token1       IN VARCHAR2 DEFAULT NULL,
                            p_token1_value IN VARCHAR2 DEFAULT NULL,
                            p_token2       IN VARCHAR2 DEFAULT NULL,
                            p_token2_value IN VARCHAR2 DEFAULT NULL,
                            p_token3       IN VARCHAR2 DEFAULT NULL,
                            p_token3_value IN VARCHAR2 DEFAULT NULL,
                            p_token4       IN VARCHAR2 DEFAULT NULL,
                            p_token4_value IN VARCHAR2 DEFAULT NULL,
                            p_token5       IN VARCHAR2 DEFAULT NULL,
                            p_token5_value IN VARCHAR2 DEFAULT NULL);

  FUNCTION app_id RETURN NUMBER;

END hw_api;
/
CREATE OR REPLACE PACKAGE BODY hw_api AS
  /*==================================================
  
  Description:
      This program provide common API for procedure
  History:
      1.00  2006-06-01  jim.lin    Creation
  ==================================================*/

  -- Global variable
  /*  l_debug VARCHAR2(1) := nvl(fnd_profile.value('AFLOG_ENABLED'),
  'N');*/

  l_debug VARCHAR2(1) := 'Y';

  PROCEDURE stack_message(p_msg VARCHAR2) IS
  BEGIN
    hw_api.set_message(upper('fnd'),
                       upper('conc-places message on stack'),
                       upper('message'),
                       p_msg);
  END stack_message;

  PROCEDURE raise_exception(p_return_status     VARCHAR2,
                            px_procedure_status IN OUT VARCHAR2) IS
  BEGIN
    IF (p_return_status = fnd_api.g_ret_sts_unexp_error) THEN
      RAISE fnd_api.g_exc_unexpected_error;
    ELSIF (p_return_status = fnd_api.g_ret_sts_error) THEN
      RAISE fnd_api.g_exc_error;
    ELSIF (p_return_status = hw_api.g_ret_sts_warning) THEN
      px_procedure_status := hw_api.g_ret_sts_warning;
    END IF;
  END raise_exception;

  PROCEDURE init_msg_list(p_init_msg_list IN VARCHAR2) IS
  BEGIN
    IF (fnd_api.to_boolean(p_init_msg_list)) THEN
      fnd_msg_pub.initialize;
    END IF;
  END init_msg_list;

  FUNCTION start_activity(p_pkg_name       IN VARCHAR2,
                          p_api_name       IN VARCHAR2,
                          p_savepoint_name IN VARCHAR2 DEFAULT NULL,
                          p_init_msg_list  IN VARCHAR2 DEFAULT fnd_api.g_false,
                          l_api_version    IN NUMBER DEFAULT NULL,
                          p_api_version    IN NUMBER DEFAULT NULL)
    RETURN VARCHAR2 IS
  BEGIN
    -- set debug indentation
    IF (l_debug = 'Y') THEN
      hw_log.set_indentation(p_pkg_name || '.' || p_api_name);
      hw_log.event('Entering ');
    END IF;
  
    -- compare api version
    IF l_api_version IS NOT NULL
       AND p_api_version IS NOT NULL THEN
      IF NOT fnd_api.compatible_api_call(l_api_version,
                                         p_api_version,
                                         p_api_name,
                                         p_pkg_name) THEN
        RETURN(fnd_api.g_ret_sts_unexp_error);
      END IF;
    END IF;
  
    -- Standard start of API savepoint
    IF p_savepoint_name IS NOT NULL THEN
      dbms_transaction.savepoint(p_savepoint_name);
    END IF;
  
    -- initialize message list
    init_msg_list(p_init_msg_list);
  
    RETURN(fnd_api.g_ret_sts_success);
  
  END start_activity;

  FUNCTION end_activity(p_pkg_name         IN VARCHAR2,
                        p_api_name         IN VARCHAR2,
                        p_commit           IN VARCHAR2 DEFAULT NULL,
                        p_procedure_status IN VARCHAR2,
                        x_msg_count        OUT NOCOPY NUMBER,
                        x_msg_data         OUT NOCOPY VARCHAR2)
    RETURN VARCHAR2 IS
    l_return_value VARCHAR2(30);
  BEGIN
    -- standard check for automatic commit
    IF p_commit IS NOT NULL THEN
      IF fnd_api.to_boolean(p_commit) THEN
        COMMIT WORK;
      END IF;
    END IF;
  
    --- Standard call to get message count and if count is 1, get message info
    fnd_msg_pub.count_and_get(p_encoded => fnd_api.g_false,
                              p_count   => x_msg_count,
                              p_data    => x_msg_data);
    IF x_msg_count > 1 THEN
      x_msg_data := fnd_msg_pub.get(p_msg_index => fnd_msg_pub.G_LAST,
                                           p_encoded   => fnd_api.g_false);
    END IF;
  
    IF p_procedure_status = hw_api.g_ret_sts_warning THEN
      l_return_value := hw_api.g_ret_sts_warning;
    ELSE
      l_return_value := fnd_api.g_ret_sts_success;
    END IF;
  
    -- reset debug indentation
    IF (l_debug = 'Y') THEN
      IF p_procedure_status = hw_api.g_ret_sts_warning THEN
        hw_log.warning('Leaving warning : ' || x_msg_data);
      ELSE
        hw_log.event('Leaving');
      END IF;
    END IF;
  
    hw_log.reset_indentation(p_pkg_name || '.' || p_api_name);
    RETURN(l_return_value);
  END end_activity;

  FUNCTION handle_exceptions(p_pkg_name       IN VARCHAR2,
                             p_api_name       IN VARCHAR2,
                             p_savepoint_name IN VARCHAR2 DEFAULT NULL,
                             p_exc_name       IN VARCHAR2,
                             x_msg_count      OUT NOCOPY NUMBER,
                             x_msg_data       OUT NOCOPY VARCHAR2)
    RETURN VARCHAR2 IS
    l_return_value VARCHAR2(30) := fnd_api.g_ret_sts_unexp_error;
    l_log_level    NUMBER := 6;
  BEGIN
    -- standard check for rollback to savepoint
    IF p_savepoint_name IS NOT NULL THEN
      dbms_transaction.rollback_savepoint(p_savepoint_name);
    END IF;
  
    IF p_exc_name = hw_api.g_exc_name_error THEN
      l_log_level    := 4;
      l_return_value := fnd_api.g_ret_sts_error;
    ELSIF p_exc_name = hw_api.g_exc_name_unexp THEN
      l_log_level := 5;
    ELSE
      -- when others exception
      IF (l_debug = 'Y') THEN
        hw_log.debug('exception:' || chr(10) || SQLERRM);
        -- 10g dbms_utility.format_error_backtrace
        hw_log.debug('error_stack:' || chr(10) ||
                     dbms_utility.format_error_backtrace);
      END IF;
    
      l_log_level := 6;
      -- substrb(SQLERRM, 240) fnd_msg_pub may occur error when sqlerrm containt multi-byte string
      IF fnd_msg_pub.check_msg_level(fnd_msg_pub.g_msg_lvl_unexp_error) THEN
        fnd_msg_pub.add_exc_msg(p_pkg_name,
                                p_api_name,
                                substrb(SQLERRM,
                                        1,
                                        240));
      END IF;
    END IF;
  
    -- get error message count and data
    fnd_msg_pub.count_and_get(p_encoded => fnd_api.g_false,
                              p_count   => x_msg_count,
                              p_data    => x_msg_data);
    IF x_msg_count > 1 THEN
      x_msg_data := fnd_msg_pub.get(p_msg_index => fnd_msg_pub.G_LAST,
                                           p_encoded   => fnd_api.g_false);
    END IF;
  
    -- reset debug indentation
    IF (l_debug = 'Y') THEN
      IF l_log_level >= 5 THEN
        hw_log.fatal('Leaving error : ' || x_msg_data);
      ELSE
        hw_log.error('Leaving error : ' || x_msg_data);
      END IF;
      hw_log.reset_indentation(p_pkg_name || '.' || p_api_name);
    END IF;
  
    RETURN(l_return_value);
  END handle_exceptions;

  PROCEDURE set_message(p_app_name     IN VARCHAR2 DEFAULT hw_api.g_app_name,
                        p_msg_name     IN VARCHAR2,
                        p_token1       IN VARCHAR2 DEFAULT NULL,
                        p_token1_value IN VARCHAR2 DEFAULT NULL,
                        p_token2       IN VARCHAR2 DEFAULT NULL,
                        p_token2_value IN VARCHAR2 DEFAULT NULL,
                        p_token3       IN VARCHAR2 DEFAULT NULL,
                        p_token3_value IN VARCHAR2 DEFAULT NULL,
                        p_token4       IN VARCHAR2 DEFAULT NULL,
                        p_token4_value IN VARCHAR2 DEFAULT NULL,
                        p_token5       IN VARCHAR2 DEFAULT NULL,
                        p_token5_value IN VARCHAR2 DEFAULT NULL) IS
  BEGIN
    fnd_message.set_name(p_app_name,
                         p_msg_name);
    IF (p_token1 IS NOT NULL) THEN
      fnd_message.set_token(token => p_token1,
                            VALUE => p_token1_value);
    END IF;
    IF (p_token2 IS NOT NULL) THEN
      fnd_message.set_token(token => p_token2,
                            VALUE => p_token2_value);
    END IF;
    IF (p_token3 IS NOT NULL) THEN
      fnd_message.set_token(token => p_token3,
                            VALUE => p_token3_value);
    END IF;
    IF (p_token4 IS NOT NULL) THEN
      fnd_message.set_token(token => p_token4,
                            VALUE => p_token4_value);
    END IF;
    IF (p_token5 IS NOT NULL) THEN
      fnd_message.set_token(token => p_token5,
                            VALUE => p_token5_value);
    END IF;
    fnd_msg_pub.add;
  END set_message;

  PROCEDURE raise_exception(p_app_name     IN VARCHAR2 DEFAULT hw_api.g_app_name,
                            p_msg_name     IN VARCHAR2 DEFAULT NULL,
                            p_token1       IN VARCHAR2 DEFAULT NULL,
                            p_token1_value IN VARCHAR2 DEFAULT NULL,
                            p_token2       IN VARCHAR2 DEFAULT NULL,
                            p_token2_value IN VARCHAR2 DEFAULT NULL,
                            p_token3       IN VARCHAR2 DEFAULT NULL,
                            p_token3_value IN VARCHAR2 DEFAULT NULL,
                            p_token4       IN VARCHAR2 DEFAULT NULL,
                            p_token4_value IN VARCHAR2 DEFAULT NULL,
                            p_token5       IN VARCHAR2 DEFAULT NULL,
                            p_token5_value IN VARCHAR2 DEFAULT NULL) IS
  BEGIN
    IF p_app_name IS NOT NULL
       AND p_msg_name IS NOT NULL THEN
      fnd_message.set_name(p_app_name,
                           p_msg_name);
      IF (p_token1 IS NOT NULL) THEN
        fnd_message.set_token(token => p_token1,
                              VALUE => p_token1_value);
      END IF;
      IF (p_token2 IS NOT NULL) THEN
        fnd_message.set_token(token => p_token2,
                              VALUE => p_token2_value);
      END IF;
      IF (p_token3 IS NOT NULL) THEN
        fnd_message.set_token(token => p_token3,
                              VALUE => p_token3_value);
      END IF;
      IF (p_token4 IS NOT NULL) THEN
        fnd_message.set_token(token => p_token4,
                              VALUE => p_token4_value);
      END IF;
      IF (p_token5 IS NOT NULL) THEN
        fnd_message.set_token(token => p_token5,
                              VALUE => p_token5_value);
      END IF;
      app_exception.raise_exception;
    ELSE
      app_exception.raise_exception(exception_type => p_app_name,
                                    exception_code => SQLCODE,
                                    exception_text => SQLERRM);
    END IF;
  END raise_exception;

  FUNCTION app_id RETURN NUMBER IS
    CURSOR cur_app IS
      SELECT fa.application_id
        FROM fnd_application fa
       WHERE fa.application_short_name = g_app_name;
  BEGIN
    IF g_app_id IS NULL THEN
      OPEN cur_app;
      FETCH cur_app
        INTO g_app_id;
      CLOSE cur_app;
    END IF;
    RETURN g_app_id;
  END app_id;

BEGIN
  -- initial event enable varray
  init_event_enable := event_enable_type('F',
                                         'F',
                                         'F',
                                         'F',
                                         'F',
                                         'F');
END hw_api;
/
