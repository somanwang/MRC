CREATE OR REPLACE PACKAGE hw_log AS
  /*==================================================
  Copyright (C) Hand Enterprise Solutions Co.,Ltd.
             AllRights Reserved
  ==================================================*/
  /*==================================================
  Program Name:
      HSS_LOG
  Description:
      This program provide API to perform:
           log debug message
  History:
      1.00  2006-06-01  jim.lin    Creation
  ==================================================*/

  /*=============================================
  Procedure Name:
      set_indentation
  Description:
      This procedure perform set debug indentation
  Argument:
      p_proc_name :  procedure name, may be package name + '.' + procedure name
  History:
      1.00  2006-06-01  jim.lin    Creation
  =============================================*/
  PROCEDURE set_indentation(p_proc_name IN VARCHAR2);

  /*=============================================
  Procedure Name:
      reset_indentation
  Description:
      This procedure perform reset debug indentation
  Argument:
      p_proc_name :  procedure name, may be package name + '.' + procedure name
  History:
      1.00  2006-06-01  jim.lin    Creation
  =============================================*/
  PROCEDURE reset_indentation(p_proc_name IN VARCHAR2);

  /*=============================================
  Procedure Name:
      String
  Description:
      This procedure perform log message
  Argument:
      p_msg      : log message
      p_level    : log level
                     1  STATEMENT
                     2  PROCEDURE
                     3  Event
                     4  EXCEPTION
                     5  Error
                     6  Unexpected
      p_module   : log module
  History:
      1.00  2006-06-01  jim.lin    Creation
  =============================================*/
  PROCEDURE STRING(p_msg    IN VARCHAR2,
                   p_module IN VARCHAR2 DEFAULT 'HSS',
                   p_level  IN NUMBER DEFAULT 1);

  PROCEDURE debug(p_msg    IN VARCHAR2,
                  p_module IN VARCHAR2 DEFAULT 'HSS');

  PROCEDURE info(p_msg    IN VARCHAR2,
                 p_module IN VARCHAR2 DEFAULT 'HSS');

  PROCEDURE event(p_msg    IN VARCHAR2,
                  p_module IN VARCHAR2 DEFAULT 'HSS');

  PROCEDURE warning(p_msg    IN VARCHAR2,
                    p_module IN VARCHAR2 DEFAULT 'HSS');

  PROCEDURE error(p_msg    IN VARCHAR2,
                  p_module IN VARCHAR2 DEFAULT 'HSS');

  PROCEDURE fatal(p_msg    IN VARCHAR2,
                  p_module IN VARCHAR2 DEFAULT 'HSS');

END hw_log;
/
CREATE OR REPLACE PACKAGE BODY hw_log AS
  /*==================================================
  
  Description:
      This program provide API to perform:
           log debug message
  History:
      1.00  2006-06-01  jim.lin    Creation
  ==================================================*/

  -- Constants
  g_log_mode VARCHAR2(30) := nvl(fnd_profile.value('HSS_LOG_MODE'),
                                 'FILE');
  g_max_indent_str_len CONSTANT NUMBER(4) := 1880;

  -- Type define
  TYPE proc_tbl IS TABLE OF VARCHAR2(80) INDEX BY BINARY_INTEGER;

  -- global variable
  g_debug_init   BOOLEAN := FALSE; -- Log initialize state
  g_fd           utl_file.file_type; -- Log file descriptor
  g_file_dbg_on  NUMBER := 0; -- Log ON state
  g_cp_flag      NUMBER := 0; -- conc program
  g_file_postfix VARCHAR2(100) := fnd_global.user_name;

  g_indent_str VARCHAR2(4000) := NULL;
  g_proc       VARCHAR2(80);
  g_proc_tbl   proc_tbl;
  g_index      NUMBER := 0;

  --  Debug Enabled
  l_debug VARCHAR2(1) := nvl(fnd_profile.value('AFLOG_ENABLED'),
                             'N');

  PROCEDURE set_indentation(p_proc_name VARCHAR2) IS
  BEGIN
    --
    -- Set the global procedure name
    --
    g_proc := p_proc_name;
    --
    -- Increase the indent string by 2 spaces.
    --
    g_indent_str := g_indent_str || '..';
    IF length(g_indent_str) > g_max_indent_str_len THEN
      g_indent_str := '..';
    END IF;
    g_index := g_index + 1;
    g_proc_tbl(g_index) := p_proc_name;
  
  END set_indentation;

  PROCEDURE reset_indentation(p_proc_name VARCHAR2) IS
  BEGIN
  
    IF p_proc_name = g_proc THEN
      --
      -- Reset the indent string by 2 space.
      --
      g_indent_str := substr(g_indent_str,
                             1,
                             length(g_indent_str) - 2);
      --
      -- Drop the current called procedure name from the stack since
      -- we just exited.
      --
      g_proc_tbl.delete(g_index);
      --
      -- Get the parent calling procedure name from the stack.
      --
      g_index := g_index - 1;
      IF g_index <= 0 THEN
        g_index := 0;
        -- Reset the proc name otherwise it prints the last proc name
        -- if the caller didn't make call to set_indentation.
        g_proc := NULL;
      ELSE
        g_proc := g_proc_tbl(g_index);
      END IF;
    END IF;
  END reset_indentation;

  PROCEDURE log_init IS
    l_dbgfile       VARCHAR2(256);
    l_errmsg        VARCHAR2(256);
    l_dbgpath       VARCHAR2(128);
    l_ndx           NUMBER;
    l_strlen        NUMBER;
    l_dbgdir        VARCHAR2(256);
    l_dir_separator VARCHAR2(1);
  BEGIN
    g_file_dbg_on := 1;
    SELECT nvl(fnd_profile.value('CONC_REQUEST_ID'),
               0)
      INTO g_cp_flag
      FROM dual;
    SELECT to_char(SYSDATE,
                   'YYYY-MM-DD HH24.MI.SS')
      INTO l_errmsg
      FROM dual;
    IF (g_cp_flag > 0 AND g_log_mode <> 'FILEONLY') THEN
      fnd_file.put_line(fnd_file.log,
                        ' ******** New Session. : ' || l_errmsg ||
                        ' **********');
    ELSE
      SELECT fnd_profile.value('HSS_DEBUG_FILE') INTO l_dbgpath FROM dual;
      IF l_dbgpath IS NULL THEN
        l_dbgpath := '/usr/tmp/hw' || userenv('SESSIONID') || '.log';
        /*        g_debug_init := TRUE;
        RAISE utl_file.invalid_path;*/
      END IF;
    
      -- Seperate the filename from the directory
      l_strlen        := length(l_dbgpath);
      l_dbgfile       := l_dbgpath;
      l_dir_separator := '/';
      --Check if separator exits, could be different depending on os
      l_ndx := instr(l_dbgfile,
                     l_dir_separator);
      IF (l_ndx = 0) THEN
        l_dir_separator := '\';
      END IF;
    
      LOOP
        l_ndx := instr(l_dbgfile,
                       l_dir_separator);
        EXIT WHEN((l_ndx = 0) OR (l_ndx IS NULL));
        l_dbgfile := substr(l_dbgfile,
                            l_ndx + 1,
                            l_strlen - l_ndx + 1);
      END LOOP;
    
      l_dbgdir := substr(l_dbgpath,
                         1,
                         l_strlen - length(l_dbgfile) - 1);
    
      IF g_file_postfix IS NOT NULL THEN
        l_ndx := instr(l_dbgfile,
                       '.');
        IF l_ndx > 0 THEN
          l_dbgfile := substr(l_dbgfile,
                              1,
                              l_ndx - 1) || '_' || g_file_postfix ||
                       substr(l_dbgfile,
                              l_ndx);
        ELSE
          l_dbgfile := l_dbgfile || '_' || g_file_postfix || '.log';
        END IF;
      END IF;
      -- Open Log file
      g_fd := utl_file.fopen(l_dbgdir,
                             l_dbgfile,
                             'a');
      utl_file.put_line(g_fd,
                        '');
      utl_file.put_line(g_fd,
                        ' ******** New Session. : ' || l_errmsg ||
                        ' **********');
    END IF;
  
    g_debug_init := TRUE;
  
  EXCEPTION
    WHEN utl_file.invalid_path THEN
      g_file_dbg_on := 0;
    WHEN OTHERS THEN
      g_file_dbg_on := 0;
  END log_init;

  PROCEDURE log_file(p_msg IN VARCHAR2) IS
  BEGIN
  
    IF (g_file_dbg_on = 1) THEN
      --If called from a concurrent program add msg to FND log
      IF (g_cp_flag > 0 AND g_log_mode <> 'FILEONLY') THEN
        fnd_file.put_line(fnd_file.log,
                          p_msg);
      ELSE
        utl_file.put_line(g_fd,
                          p_msg);
        utl_file.fflush(g_fd);
      END IF;
    END IF;
  
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END log_file;

  PROCEDURE STRING(p_msg    IN VARCHAR2,
                   p_module IN VARCHAR2 DEFAULT 'HSS',
                   p_level  IN NUMBER DEFAULT 1) IS
  BEGIN
  
    -- always log 
    /*    IF (g_cp_flag <= 0) THEN
    
      IF l_debug <> 'Y' THEN
        RETURN;
      END IF;
    
      IF NOT fnd_log.test(p_level,
                          p_module) THEN
        RETURN;
      END IF;
    END IF;*/
  
    IF (g_debug_init = FALSE) THEN
      log_init;
    END IF;
  
    IF (g_cp_flag > 0)
       OR (g_log_mode = 'FILE' OR g_log_mode = 'FILEONLY') THEN
      IF instr(p_msg,
               fnd_api.g_miss_char) > 0 THEN
        log_file(to_char(systimestamp,
                         'HH24:MI:SSXFF5') || g_indent_str || g_proc ||
                 ' : ' || REPLACE(p_msg,
                                  fnd_api.g_miss_char,
                                  '?'));
      ELSE
        log_file(to_char(systimestamp,
                         'HH24:MI:SSXFF5') || g_indent_str || g_proc ||
                 ' : ' || p_msg);
      END IF;
    ELSE
      -- logging into fnd_log_message
      IF instr(p_msg,
               fnd_api.g_miss_char) > 0 THEN
        fnd_log.string(p_level,
                       p_module,
                       to_char(systimestamp,
                               'HH24:MI:SSXFF5') || g_indent_str || g_proc ||
                       ' : ' || REPLACE(p_msg,
                                        fnd_api.g_miss_char,
                                        '?'));
      ELSE
        fnd_log.string(p_level,
                       p_module,
                       to_char(systimestamp,
                               'HH24:MI:SSXFF5') || g_indent_str || g_proc ||
                       ' : ' || p_msg);
      END IF;
    END IF;
  END STRING;

  PROCEDURE debug(p_msg    IN VARCHAR2,
                  p_module IN VARCHAR2 DEFAULT 'HSS') IS
  BEGIN
    hw_log.string(p_msg    => p_msg,
                  p_module => p_module,
                  p_level  => 1);
  END debug;

  PROCEDURE info(p_msg    IN VARCHAR2,
                 p_module IN VARCHAR2 DEFAULT 'HSS') IS
  BEGIN
    hw_log.string(p_msg    => p_msg,
                  p_module => p_module,
                  p_level  => 2);
  END info;

  PROCEDURE event(p_msg    IN VARCHAR2,
                  p_module IN VARCHAR2 DEFAULT 'HSS') IS
  BEGIN
    hw_log.string(p_msg    => p_msg,
                  p_module => p_module,
                  p_level  => 2);
  END event;

  PROCEDURE warning(p_msg    IN VARCHAR2,
                    p_module IN VARCHAR2 DEFAULT 'HSS') IS
  BEGIN
    hw_log.string(p_msg    => p_msg,
                  p_module => p_module,
                  p_level  => 3);
  END warning;

  PROCEDURE error(p_msg    IN VARCHAR2,
                  p_module IN VARCHAR2 DEFAULT 'HSS') IS
  BEGIN
    hw_log.string(p_msg    => p_msg,
                  p_module => p_module,
                  p_level  => 5);
  END error;

  PROCEDURE fatal(p_msg    IN VARCHAR2,
                  p_module IN VARCHAR2 DEFAULT 'HSS') IS
  BEGIN
    hw_log.string(p_msg    => p_msg,
                  p_module => p_module,
                  p_level  => 6);
  END fatal;

END hw_log;
/
