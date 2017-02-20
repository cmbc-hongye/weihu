#!/usr/bin/env python
#####################################################################
#coding:utf-8                                                       #
#           Programe : check Oracle Database                        #
#           Author   : Hong Ye                                      #
#           Python Versin : 2.7                                     #
#           Version :                                               #
#             1.0 :  Initially Script         2016-08-17            #
#             1.1 :  Fix Performance Bug      2016-09-26            #
#             1.2 :  Add Rac & Mutli Process                        #
#                                                                   #
#####################################################################
import json
import sys,os,time
import string,socket,struct
import simplejson
import datetime,atexit
from optparse import OptionParser
import cx_Oracle
 
def checkOracle(dbname,username,password,ip,port,oramode=''):
    # connect 
    try:
        #con=cx_Oracle.connect(username,password,ip + ':' + port + '/' + dbname,auth)
        dsn_tns = cx_Oracle.makedsn(ip, port, dbname)
        if (oramode=='SYSDBA' or oramode=='SYSOPER') :
            con = cx_Oracle.connect(user=username,password=password,dsn=dsn_tns,mode=oramode)
        else :
            con = cx_Oracle.connect(user=username,password=password,dsn=dsn_tns)
    except Exception,e:
        s=sys.exc_info()
        print e
        #print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)
        sys.exit(-1)

    try:
        now = datetime.datetime.now()
        otherStyleTime = now.strftime("%Y-%m-%d-%H.%M.%S")
        filename = "Oracle_" + ip + "_" + dbname + "_" + str(otherStyleTime) + ".txt"
        f=open(filename,'w')
    except Exception,e:
        s=sys.exc_info()
        print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)
        sys.exit(-1)

    cur = con.cursor()    
    Table_exits_Query = "select count(*) from all_tables where table_name='DB_BASELINE'"
      
    try: 
        cur.execute(Table_exits_Query)
        for result in cur:
            if result[0] == 0 :
                x=cur.execute("create table DB_BASELINE as select name,value from v$sysstat where 1=0")  
    except Exception,e:
        s=sys.exc_info()
        print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)
    
    #info-info
    info_Query = """SELECT DBID,b.name,instance_name, VERSION,INSTANCE_NUMBER,PARALLEL,
                    TO_CHAR (startup_time, \'yyyy-mm-dd hh24:mi:ss\') startup_time,
                    open_mode,a.status,  log_mode, database_status,SHUTDOWN_PENDING
                    FROM gv$instance a, gv$database b
                    where a.inst_id=b.inst_id
                    """
    cur.execute(info_Query)
    for result in cur:
        f.write( "oracle.dbid|" + str(result[0]) + '|\n')
        f.write( "oracle.dbname|" + str(result[1]) + '|\n')
        f.write( "oracle.instname|" + str(result[2]) + '|\n')
        f.write( "oracle.version|" + str(result[3]) + '|\n')
        f.write( "oracle.instance_number|" + str(result[4]) + '|\n')
        f.write( "oracle.parallel|" + str(result[5]) + '|\n')
        f.write( "oracle.uptime|" + str(result[6]) + '|\n')
        f.write( "oracle.open_mode|" + str(result[7]) + '|\n')
        f.write( "oracle.status|" + str(result[8]) + '|\n')
        f.write( "oracle.logmode|" + str(result[9]) + '|\n')
        f.write( "oracle.dbstatus|" + str(result[10]) + '|\n')
        #f.write( "oracle.inst_mode|" + str(result[11]) + '|\n')
        f.write( "oracle.shutdown_pending|" + str(result[11]) + '|\n')
        
    dbsize_Query = """select round(sum(bytes)/1024/1024/1024)||'G' dbsize from dba_data_files"""
    cur.execute(dbsize_Query)     
    for result in cur:          
        f.write( "oracle.dbsize|" + str(result[0]) + '|\n')
        
    total_memory_Query = """select ((SELECT ROUND(sum(value)/1024/1024,2) from v$sga)+(select ROUND(value/1024/1024,2) from v$pgastat where name='total PGA allocated'))||'M' AS "SIZE(M)" FROM DUAL"""
    cur.execute(total_memory_Query)   
    for result in cur:            
        f.write( "oracle.total_memory|" + str(result[0]) + '|\n')
        
    archive_log_used_Query = "select PERCENT_SPACE_USED from v$flash_recovery_area_usage where FILE_TYPE='ARCHIVED LOG'"
    cur.execute(archive_log_used_Query)  
    for result in cur:             
        f.write( "oracle.archive_log_used|" + str(result[0]) + '|\n')
    
    tbspacelist = []
    try:
        TBSP_STMT = """SELECT A.TABLESPACE_NAME, 
               BLOCK_SIZE,STATUS,
               CONTENTS,LOGGING,EXTENT_MANAGEMENT,ALLOCATION_TYPE,
               SEGMENT_SPACE_MANAGEMENT,
               SPACE || 'M' "SUM_SPACE(M)", 
               BLOCKS "SUM_BLOCKS", 
               SPACE - NVL (FREE_SPACE, 0) || 'M' "USED_SPACE(M)", 
               ROUND ( (1 - NVL (FREE_SPACE, 0) / SPACE) * 100, 2) || '%' 
                  "USED_RATE(%)", 
               FREE_SPACE || 'M' "FREE_SPACE(M)" 
          FROM (  SELECT TABLESPACE_NAME, 
                         ROUND (SUM (BYTES) / (1024 * 1024), 2) SPACE, 
                         SUM (BLOCKS) BLOCKS 
                    FROM DBA_DATA_FILES 
                GROUP BY TABLESPACE_NAME) A, 
               (  SELECT TABLESPACE_NAME, 
                         ROUND (SUM (BYTES) / (1024 * 1024), 2) FREE_SPACE 
                    FROM DBA_FREE_SPACE 
                GROUP BY TABLESPACE_NAME) B ,
                (SELECT TABLESPACE_NAME, BLOCK_SIZE, MIN_EXTLEN, STATUS, CONTENTS, 
                LOGGING, FORCE_LOGGING, EXTENT_MANAGEMENT, ALLOCATION_TYPE, PLUGGED_IN, 
                SEGMENT_SPACE_MANAGEMENT, DEF_TAB_COMPRESSION, RETENTION, BIGFILE 
                FROM DBA_TABLESPACES) C
         WHERE A.TABLESPACE_NAME = B.TABLESPACE_NAME(+) 
           AND A.TABLESPACE_NAME = C.TABLESPACE_NAME(+) 
        UNION ALL             
        SELECT D.TABLESPACE_NAME, 
               BLOCK_SIZE,STATUS,
               CONTENTS,LOGGING,EXTENT_MANAGEMENT,ALLOCATION_TYPE,
               SEGMENT_SPACE_MANAGEMENT,
               SPACE || 'M' "SUM_SPACE(M)", 
               BLOCKS SUM_BLOCKS, 
               USED_SPACE || 'M' "USED_SPACE(M)", 
               ROUND (NVL (USED_SPACE, 0) / SPACE * 100, 2) || '%' "USED_RATE(%)", 
               NVL (FREE_SPACE, 0) || 'M' "FREE_SPACE(M)" 
          FROM (  SELECT TABLESPACE_NAME, 
                         ROUND (SUM (BYTES) / (1024 * 1024), 2) SPACE, 
                         SUM (BLOCKS) BLOCKS 
                    FROM DBA_TEMP_FILES 
                GROUP BY TABLESPACE_NAME) D, 
               (  SELECT TABLESPACE_NAME, 
                         ROUND (SUM (BYTES_USED) / (1024 * 1024), 2) USED_SPACE, 
                         ROUND (SUM (BYTES_FREE) / (1024 * 1024), 2) FREE_SPACE 
                    FROM V$TEMP_SPACE_HEADER 
                GROUP BY TABLESPACE_NAME) E,
               (SELECT TABLESPACE_NAME, BLOCK_SIZE, MIN_EXTLEN, STATUS, CONTENTS, 
                LOGGING, FORCE_LOGGING, EXTENT_MANAGEMENT, ALLOCATION_TYPE, PLUGGED_IN, 
                SEGMENT_SPACE_MANAGEMENT, DEF_TAB_COMPRESSION, RETENTION, BIGFILE 
                FROM DBA_TABLESPACES) F
         WHERE D.TABLESPACE_NAME = E.TABLESPACE_NAME(+) 
           AND D.TABLESPACE_NAME = F.TABLESPACE_NAME(+) 
           ORDER BY 1"""
        cur.execute(TBSP_STMT)    
        for result in cur:
            tbspace = str(result[0]) 
            f.write( "oracle.block_size[" + tbspace + "]|" + str(result[1]) + '|\n')
            f.write( "oracle.status[" + tbspace + "]|" + str(result[2]) + '|\n')
            f.write( "oracle.contents[" + tbspace + "]|" + str(result[3]) + '|\n')
            f.write( "oracle.logging[" + tbspace + "]|" + str(result[4]) + '|\n')
            f.write( "oracle.extent_management[" + tbspace + "]|" + str(result[5]) + '|\n')
            f.write( "oracle.allocation_type[" + tbspace + "]|" + str(result[6]) + '|\n')
            f.write( "oracle.segment_space_management[" + tbspace + "]|" + str(result[7]) + '|\n')
            f.write( "oracle.sum_space[" + tbspace + "]|" + str(result[8]) + '|\n')
            f.write( "oracle.sum_blocks[" + tbspace + "]|" + str(result[9]) + '|\n')
            f.write( "oracle.used_space[" + tbspace + "]|" + str(result[10]) + '|\n')
            f.write( "oracle.used_rate[" + tbspace + "]|" + str(result[11]) + '|\n')
            f.write( "oracle.free_space[" + tbspace + "]|" + str(result[12]) + '|\n')     
            tbspacelist += [{'{#TBSPNAME}':str(result[0])}]
    except Exception as e:
        s=sys.exc_info()
        print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)      
    
    #Redo Logs & redo log switch 
    loggrouplist = []
    try:
        LOG_STMT = "select a.group#,sequence#,bytes,members,ARCHIVED,a.status,b.type,IS_RECOVERY_DEST_FILE,member,FIRST_CHANGE#,FIRST_TIME from v$log a,V$LOGFILE b where a.group#=b.group#"
        cur.execute(LOG_STMT)
        for result in cur:
            f.write( "oracle.sequence[" + str(result[0]) + "]|" + str(result[1]) + '|\n')
            f.write( "oracle.bytes[" + str(result[0]) + "]|" + str(result[2]) + '|\n')
            f.write( "oracle.members[" + str(result[0]) + "]|" + str(result[3]) + '|\n')
            f.write( "oracle.archived[" + str(result[0]) + "]|" + str(result[4]) + '|\n')
            f.write( "oracle.status[" + str(result[0]) + "]|" + str(result[5]) + '|\n')
            f.write( "oracle.type[" + str(result[0]) + "]|" + str(result[6]) + '|\n')
            f.write( "oracle.is_recovery_dest_file[" + str(result[0]) + "]|" + str(result[7]) + '|\n')
            f.write( "oracle.member[" + str(result[0]) + "]|" + str(result[8]) + '|\n')
            f.write( "oracle.first_change[" + str(result[0]) + "]|" + str(result[9]) + '|\n')
            f.write( "oracle.first_time[" + str(result[0]) + "]|" + str(result[10]) + '|\n')
            loggrouplist += [{'{#GROUP}':str(result[0])}]
    except Exception as e:
        s=sys.exc_info()
        print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)    
    
    try:
        DBA_PROFILE_STMT = """select RESOURCE_NAME,LIMIT from dba_profiles where RESOURCE_NAME in (        
        'PASSWORD_LIFE_TIME ',
        'PASSWORD_LOCK_TIME',
        'PASSWORD_GRACE_TIME',
        'FAILED_LOGIN_ATTEMPTS')"""   
        cur.execute(DBA_PROFILE_STMT)                                
        for result in cur:                                   
            f.write( "oracle.dba_" + str(result[0]) + "|" + str(result[1]) + '|\n')
    except Exception as e:
        s=sys.exc_info()
        print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)   
        
    try:
        PARAMETER_STMT = """select name,value from GV$PARAMETER 
                         where name in (
                         'large_pool_size',
                         'sec_case_sensitive_logon',
                         'enable_ddl_logging',
                         'sga_max_size',
                         'sga_target',
                         'share_pool_size',
                         'session_cached_cursors',
                         'deferred_segment_creation',
                         'db_recovery_file_dest_size',
                         'open_cursors',
                         'processes',
                         'db_writer_processes',
                         'parallel_force_local',
                         'max_dump_file_size ',
                         'undo_retention',
                         'parallel_max_servers',
                         'control_file_record_keep_time',
                         'deferred_segment_creation',
                         'result_cache_max_size',
                         'audit_trail',
                         'resource_limit',
                         'resource_manager_plan')
                         """
        cur.execute(PARAMETER_STMT)    
        for result in cur:
            syspara=str(result[0])
            f.write( "oracle.syspara_" + syspara + "|" + str(result[1]) + '|\n')
    except Exception as e:
        s=sys.exc_info()
        print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)      
    
    Imp_Parameter_STMT = """
        SELECT x.ksppinm NAME, y.ksppstvl VALUE
        FROM SYS.x$ksppi x, SYS.x$ksppcv y
        WHERE x.inst_id = USERENV ('Instance')
        AND y.inst_id = USERENV ('Instance')
        AND x.indx = y.indx
        AND x.ksppinm in (
        '_partition_large_extents',
        '_undo_autotune',
        '_gc_policy_time',
        '_gc_undo_affinity',
        '_gc_defer_time',
        '_optimizer_adaptive_cursor_sharing',
        '_optimizer_extended_cursor_sharing',
        '_optimizer_extended_cursor_sharing_rel',
        '_optimizer_use_feedback',
        '_px_use_large_pool',
        '_use_adaptive_log_file_sync',
        '_optimizer_null_aware_antijoin',
        '_b_tree_bitmap_plans',
        '_index_partition_large_extents',
        '_memory_imm_mode_without_autosga',
        '_bloom_filter_enable',
        '_bloom_pruning_enable',
        '_cleanup_rollback_entries',
        '_clusterwide_global_transactions',
        '_datafile_write_errors_crash_instance',
        '_enable_NUMA_support',
        '_ksmg_granule_size',
        '_optimizer_cost_based_transformation',
        '_optimizer_use_feedback')"""
    cur.execute(Imp_Parameter_STMT)    
    if (oramode=='SYSDBA' or oramode=='SYSOPER') :
        for result in cur:
            f.write( "oracle._imppara_" + str(result[0]) + "|" + str(result[1]) + '|\n')
    else:
        print "Hidden parameters info Must be Collected by SYSDBA"
        
    #connection
    session_Query="""
    select P_CUR_COUNT,P_MAX_COUNT,S_CUR_COUNT,S_MAX_COUNT 
                                from (select count(*) "P_CUR_COUNT" from v$process) a, 
                                     (select value "P_MAX_COUNT" from v$parameter where name='processes') b,
                                     (select count(*) "S_CUR_COUNT" FROM V$SESSION) c,
                                     (select value "S_MAX_COUNT" from v$parameter where name='sessions') d"""
    cur.execute(session_Query)
    for result in cur:
        f.write( "oracle.cur_process|" + str(result[0]) + '|\n')
        f.write( "oracle.total_process|" + str(result[1]) + '|\n')
        f.write( "oracle.cur_conn|" + str(result[2]) + '|\n')
        f.write( "oracle.total_conn|" + str(result[3]) + '|\n')

    #performance info
    start_time = 0
    end_time = 0
    diff_time = 0
    db_time = 0
    user_commit = 0
    user_rollbacks = 0
    execute_count = 0
    parse_hard = 0
    parse_total = 0
    session_cpu = 0
    db_block_get = 0
    db_block_changes = 0
    parse_time_cpu = 0
    parse_time_elapsed = 0
    physical_reads = 0
    session_logical_reads = 0
    physical_reads_direct = 0
    physical_reads_direct_lob = 0
    physical_writes = 0
    redo_size = 0
    sort_memory = 0
    sort_disk = 0
    sort_rows = 0
    table_scan_blocks = 0
    table_fetch_by_rowid = 0 
    consistent_gets = 0
    free_buffer_inspected = 0
    free_buffer_requested = 0
    dirty_buffers_inspected = 0
    pinned_buffers_inspected = 0
    enqueue_timeouts = 0
    exchange_deadlocks = 0
    enqueue_waits = 0
    enqueue_conversions = 0
    enqueue_requests = 0
    enqueue_releases = 0
    db_block_gets=0
    trans_Query = """select 'end time' "name",to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') value from dual
                     union all
                     select 'start time' name,to_char(to_date('1970-01-01','YYYY-MM-DD') + numtodsinterval(value,'SECOND'),'YYYY-MM-DD HH24:MI:SS')  value from DB_BASELINE where name = 'snap time'
                     union all
                     select 'diff_time' name,to_char((sysdate - to_date('01-01-1970','DD-MM-YYYY')) * (86400) - value) from DB_BASELINE where name = 'snap time'
                     union all
                     select a.name "name",to_char(a.value-b.value) "value" from v$sysstat a,DB_BASELINE b
                     where 
                     a.name=b.name 
                     and a.name in (
                     'user commits',
                     'user rollbacks',
                     'execute count',
                     'parse count (hard)',
                     'parse count (total)',
                     'CPU used by this session',
                     'db block gets',
                     'db block changes',
                     'parse time cpu',
                     'CPU used by this session',
                     'parse time elapsed',
                     'physical reads',
                     'session logical reads',
                     'physical reads direct',
                     'physical reads direct (lob)',
                     'physical writes',
                     'redo size',
                     'sorts (memory)','sorts (disk)',
                     'sorts (rows)',
                     'table fetch by rowid',
                     'consistent gets',
                     'table scan blocks gotten',
                     'free buffer inspected',
                     'free buffer requested',
                     'dirty buffers inspected',
                     'pinned buffers inspected',
                     'enqueue timeouts',
                     'exchange deadlocks',
                     'enqueue waits',
                     'enqueue conversions',
                     'enqueue requests',
                     'enqueue releases',
                     'DB time')"""               
    try:            
        cur.execute(trans_Query)
        for result in cur:
            if ( result[0] == 'user commits' ) :
                user_commit = int(result[1])
            elif ( result[0] == 'user rollbacks' ) :
                user_rollbacks = int(result[1])
            elif ( result[0] == 'execute count' ) :
                execute_count = int(result[1])
            elif ( result[0] == 'parse count (hard)' ) :
                parse_hard = int(result[1])
            elif ( result[0] == 'parse count (total)' ) :
                parse_total = int(result[1])
            elif ( result[0] == 'CPU used by this session' ) :
                session_cpu = int(result[1])
            elif ( result[0] == 'db block gets' ) :
                db_block_gets = int(result[1])
            elif ( result[0] == 'db block changes' ) :
                db_block_changes = int(result[1])
            elif ( result[0] == 'parse time cpu' ) :
                parse_time_cpu = int(result[1])
            elif ( result[0] == 'parse time elapsed' ) :
                parse_time_elapsed = int(result[1])
            elif ( result[0] == 'physical reads' ) :
                physical_reads = int(result[1])
            elif ( result[0] == 'session logical reads' ) :
                session_logical_reads = int(result[1])
            elif ( result[0] == 'physical reads direct' ) :
                physical_reads_direct = int(result[1])
            elif ( result[0] == 'physical reads direct (lob)' ) :
                physical_reads_direct_lob = int(result[1])
            elif ( result[0] == 'physical writes' ) :
                physical_writes = int(result[1])
            elif ( result[0] == 'redo size' ) :
                redo_size = int(result[1])
            elif ( result[0] == 'sorts (memory)' ) :
                sort_memory = int(result[1])
            elif ( result[0] == 'sorts (disk)' ) :
                sort_disk = int(result[1])
            elif ( result[0] == 'sorts (rows)' ) :
                sort_rows = int(result[1])
            elif ( result[0] == 'table fetch by rowid' ) :
                table_fetch_by_rowid = int(result[1])
            elif ( result[0] == 'enqueue deadlocks' ) :
                enqueue_deadlocks = int(result[1])
            elif ( result[0] == 'enqueue waits' ) :
                enqueue_timeouts = int(result[1])
            elif ( result[0] == 'enqueue conversions' ) :
                enqueue_timeouts = int(result[1])
            elif ( result[0] == 'enqueue requests' ) :
                enqueue_timeouts = int(result[1])
            elif ( result[0] == 'enqueue releases' ) :
                enqueue_timeouts = int(result[1])
            elif ( result[0] == 'enqueue timeouts' ) :
                enqueue_timeouts = int(result[1])
            elif ( result[0] == 'end time' ) :
                end_time = result[1]
            elif ( result[0] == 'start time' ) :
                start_time = result[1]
            elif ( result[0] == 'diff time' ) :
                diff_time = int(result[1])
            elif ( result[0] == 'DB time' ) :
                db_time = int(result[1])
            elif ( result[0] == 'table scan blocks gotten'):
                table_scan_blocks = int(result[1])
        f.write( "oracle.start_time|%s|\n" % start_time )
        f.write( "oracle.end_time|%s|\n" % end_time )
        f.write( "oracle.diff_time|%s|\n" % diff_time )
        f.write( "oracle.db_time|%s|\n" % db_time )
        f.write( "oracle.trans_execution_time_s|%d|\n" % (db_time/(user_commit+user_rollbacks+1)))
        f.write( "oracle.sql_execution_time_s|%d|\n" % (db_time/(execute_count+1)))
        f.write( "oracle.tpm|%d|\n" % ((user_commit+user_rollbacks)/60/(db_time+1)))
        f.write( "oracle.sql_per_trans|%d|\n" % (execute_count/(user_commit+user_rollbacks+1)))
        f.write( "oracle.trans_secuess_percent|%d|\n" % (user_commit/(user_rollbacks+1)))
        f.write( "oracle.db_time|%d|\n" % (table_scan_blocks/(consistent_gets + 1)))
        f.write( "oracle.buffer_hitratio|%.1f%%|\n" % (100 - 100.0*(physical_reads-physical_reads_direct-physical_reads_direct_lob)/(db_block_gets + consistent_gets - physical_reads_direct - physical_reads_direct_lob + 1)))
        f.write( "oracle.hard_parse_percent|%.1f%%|\n" % ( 100 - 100.0*int(parse_hard)/(int(parse_total) + 1)))
        f.write( "oracle.sort_overflow_percent|%.1f%%|\n" % ( 100.0 * int(sort_memory) / ( int(sort_memory) + int(sort_disk) + 1 )))
        f.write( "oracle.parse_percent|%.1f%%|\n" % (100 - 100.0*int(parse_total)/(int(execute_count) +1 )))
        f.write( "oracle.parse_per_session|%.1f|\n" % (100 - 100.0*int(parse_time_cpu) / (int(session_cpu) + 1)))
        f.write( "oracle.parse_time_cpu_percent|%.1f%%|\n" % (100 - 100.0*int(parse_time_cpu) / (int(parse_time_elapsed) + 1)))
        f.write( "oracle.db_block_changes_per_trans|%.1f|\n" % (int(db_block_changes) / (int(user_commit) + int(user_rollbacks) + 1)))
        f.write( "oracle.db_block_changes_perread|%.1f|\n" % (100 - 100.0*int(db_block_changes) / (int(session_logical_reads) +1 )))
        f.write( "oracle.sort_percent|%.1f%%|\n" % (int(sort_rows) / ( int(sort_memory) + int(sort_disk) + 1)))
    except Exception as e:
        s=sys.exc_info()
        print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)  

    dbtime = """
             with db_time as 
             (
             select value from v$sys_time_model
             where stat_name = 'DB time')
             select stm.stat_name as statistic,
       trunc(stm.value/10000000,3) as seconds,
       trunc(stm.value/(tot.value+1)*100,1) as "%"
       from v$sys_time_model stm,db_time tot
       where stm.stat_name <> 'DB time'
       and stm.value > 0
       order by stm.value desc"""
    cur.execute(dbtime)
    for result in cur:
        if ( result[0] == 'background elapsed time' ) :
            f.write( "oracle.back_total_time|" + str(result[1]) + '|\n')
        elif ( result[0] == 'background cpu time' ) :
            f.write( "oracle.back_cpu_time|" + str(result[1]) + '|\n')
        elif ( result[0] == 'sql execute elapsed time' ) :
            f.write( "oracle.sql_exec_time|" + str(result[1]) + '|\n')
        elif ( result[0] == 'parse time elapsed' ) :
            f.write( "oracle.parse_time|" + str(result[1]) + '|\n')
        elif ( result[0] == 'hard parse elapsed time' ) :
            f.write( "oracle.hard_parse_time|" + str(result[1]) + '|\n')
        elif ( result[0] == 'PL/SQL execution elapsed time' ) :
            f.write( "oracle.plsql_exec_time|" + str(result[1]) + '|\n')
        elif ( result[0] == 'PL/SQL compilation elapsed time' ) :
            f.write( "oracle.plsql_compile_time|" + str(result[1]) + '|\n')
        elif ( result[0] == 'connection management call elapsed time' ) :
            f.write( "oracle.conn_call_time|" + str(result[1]) + '|\n')
        elif ( result[0] == 'sequence load elapsed time' ) :
            f.write( "oracle.seq_load_time|" + str(result[1]) + '|\n')
        elif ( result[0] == 'repeated bind elapsed time' ) :
            f.write( "oracle.rep_bind_time|" + str(result[1]) + '|\n')
        elif ( result[0] == 'failed parse elapsed time' ) :
            f.write( "oracle.fail_parse_time|" + str(result[1]) + '|\n')
            
    ##io
    waits_event_Query="""SELECT 
                        to_char(sum(decode(event,'direct path read',total_waits,0))) DirectPathRead,
                        to_char(sum(decode(event,'file identify',total_waits, 'file open',total_waits,0))) FileIO,
                        to_char(sum(decode(event,'log file single write',total_waits, 'log file parallel write',total_waits,0))) LogWrite,
                        to_char(sum(decode(event,'db file scattered read',total_waits,0))) MultiBlockRead,
                        to_char(sum(decode(event,'control file sequential read',0,'control file single write',0,'control file parallel write',0,
                        'db file sequential read',0,'db file scattered read',0,'direct path read',0,'file identify',0,'file open',0,
                        'SQL*Net message to client',0,'SQL*Net message to dblink',0, 'SQL*Net more data to client',0,
                        'SQL*Net more data to dblink',0, 'SQL*Net break/reset to client',0,'SQL*Net break/reset to dblink',0,
                        'log file single write',0,'log file parallel write',0,total_waits))) Other,
                        to_char(sum(decode(event,'db file sequential read',total_waits,0))) SingleBlockRead,
                        to_char(sum(decode(event,'SQL*Net message to client',total_waits,'SQL*Net message to dblink',total_waits,
                        'SQL*Net more data to client',total_waits,'SQL*Net more data to dblink',total_waits,'SQL*Net break/reset to client',
                        total_waits,'SQL*Net break/reset to dblink',total_waits,0))) SQLNET
                        FROM v$system_event WHERE event not in (
                        'SQL*Net message from client',
                        'SQL*Net more data from client',
                        'pmon timer', 'rdbms ipc message',
                        'rdbms ipc reply', 'smon timer')"""
    cur.execute(waits_event_Query)
    for result in cur:
        f.write( "oracle.wait_direct_read|" + str(result[0]) + '|\n')
        f.write( "oracle.wait_fileio|" + str(result[1]) + '|\n')
        f.write( "oracle.wait_logwrite|" + str(result[2]) + '|\n')
        f.write( "oracle.wait_multiBread|" + str(result[3]) + '|\n')
        f.write( "oracle.wait_other|" + str(result[4]) + '|\n')
        f.write( "oracle.wait_singleBread|" + str(result[5]) + '|\n')
        f.write( "oracle.wait_sqlnet|" + str(result[6]) + '|\n')
        
    #memory
    #v$sga_dynamic_components
    sga = """
    SELECT 
    'buffer_cache' "name",
    to_char(ROUND(SUM(decode(a.pool,NULL,decode(a.name,'db_block_buffers',(a.bytes)/(1024*1024),'buffer_cache',(a.bytes)/(1024*1024),0),0)),2)) "Size" ,
    1-(sum(decode(b.name, 'physical reads', b.value, 0))/
             (sum(decode(b.name, 'db block gets', b.value, 0))+
             (sum(decode(b.name, 'consistent gets', b.value, 0))))) "Hit Rate"
    FROM V$SGASTAT a, v$sysstat b
    union all
    select
    'log_buffer' "name",
    TO_CHAR(ROUND(SUM(decode(a.pool,NULL,decode(a.name,'log_buffer',(a.bytes)/(1024*1024),0),0)),2)) "Size",
    1 - sum(decode(b.name, 'redo buffer allocation retries', b.value, 0))/sum(decode(b.name, 'redo entries', b.value, 0)) "Hit Rate" 
    from V$SGASTAT a,v$sysstat b
    union all
    select
    'shared pool' "name",
    (select TO_CHAR(ROUND(SUM(decode(a.pool,'shared pool',decode(a.name,'library cache',0,'dictionary cache',0,'free memory',0,'sql area',0,(a.bytes)/(1024*1024)),0)),2)) shared_pool
    FROM V$SGASTAT A) "Size",
    (select (1- ROUND(A.BYTES /(B.BYTES *1024*1024),2))*100
    FROM V$SGASTAT A,v$sgainfo B 
    WHERE B.NAME = 'Shared Pool Size' and A.NAME= 'free memory' AND A.POOL = 'shared pool') "Hit Rate"
    from dual
    union all
    select 'pool_dict_cache' "name",
    TO_CHAR(ROUND(SUM(decode(pool,'shared pool',decode(name,'dictionary cache',(bytes)/(1024*1024),0),0)),2)) "Size",
    (SUM(GETS - GETMISSES - FIXED))/SUM(GETS) "Hit Rate" from v$ROWCACHE, V$SGASTAT
    union all
    select
    'pool_lib_cache' "name",
    TO_CHAR(ROUND(SUM(decode(pool,'shared pool',decode(name,'library cache',(bytes)/(1024*1024),0),0)),2)) "Size",
    SUM(pinhits)/sum(pins) "Hit Rate"
    FROM V$SGASTAT A, V$LIBRARYCACHE B
    """
    cur.execute(sga)
    for result in cur:
        if ( result[0] == 'buffer_cache' ) :
            f.write( "oracle.buffer_cache_size|" + str(result[1]) + '|\n')
            f.write( "oracle.buffer_cache_hirratio|" + str(result[2]) + '|\n')
        if ( result[0] == 'log_buffer' ) :
            f.write( "oracle.log_buffer_size|" + str(result[1]) + '|\n')
            f.write( "oracle.log_buffer_hirratio|" + str(result[2]) + '|\n')
        if ( result[0] == 'shared pool' ) :
            f.write( "oracle.shared_pool_size|" + str(result[1]) + '|\n')
            f.write( "oracle.shared_pool_hitratio|" + str(result[2]) + '|\n')
        if ( result[0] == 'pool_dict_cache' ) :
            f.write( "oracle.pool_dict_cache_size|" + str(result[1]) + '|\n')
            f.write( "oracle.pool_dict_cache_hitratio|" + str(result[2]) + '|\n')
        if ( result[0] == 'pool_lib_cache' ) :
            f.write( "oracle.pool_lib_cache_size|" + str(result[1]) + '|\n')
            f.write( "oracle.pool_lib_cache_hitratio|" + str(result[2]) + '|\n')
            
    #current execute sql
    sqllist=[]
    top5_execsql = """select *
                     from (select v.sql_id,
                     v.child_number,
                     v.sql_text,
                     v.elapsed_time,
                     v.cpu_time,
                     v.disk_reads,
                     rank() over(order by v.elapsed_time desc) elapsed_rank
                     from v$sql v) a
                     where elapsed_rank <= 5"""
    cur.execute(top5_execsql)
    for result in cur:
        f.write("oracle.child_number[" + str(result[0]) + "]|" + str(result[1]) + '|\n')
        f.write("oracle.sql_text[" + str(result[0]) + "]|" + str(result[2]) + '|\n')
        f.write("oracle.elapsed_time[" + str(result[0]) + "]|" + str(result[3]) + '|\n')
        f.write("oracle.cpu_time[" + str(result[0]) + "]|" + str(result[4]) + '|\n')
        f.write("oracle.disk_reads[" + str(result[0]) + "]|" + str(result[1]) + '|\n')
        f.write("oracle.elapsed_rank[" + str(result[0]) + "]|" + str(result[1]) + '|\n')
        sqllist += [{'{#SQLID}':str(result[0])}]
        
        
    ##############################################################################################
    tablespace_stats_Query = """select a.tablespace_name ,
                             case when b.count is null then 0
                                  else b.count end "count",
                             case when b.LAST_ANALYZED is null then to_date('1970-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss')
                                  else b.LAST_ANALYZED end "LAST_ANALYZED",
                             case when b.FIRST_ANALYZED is null then to_date('1970-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss')
                                  else b.FIRST_ANALYZED  end "FIRST_ANALYZED",
                             case when b.MAX_ROWS is null then 0
                                  else b.MAX_ROWS end "MAX_ROWS",
                             case when b.MAX_BLOCKS is null then 0
                                  else b.MAX_BLOCKS end "MAX_BLOCKS"     
                             from DBA_TABLESPACES a left join
                             (SELECT tablespace_name,count(*) as count,max(NUM_ROWS) "MAX_ROWS",max(BLOCKS) "MAX_BLOCKS",
                             max(LAST_ANALYZED) "LAST_ANALYZED",min(LAST_ANALYZED) "FIRST_ANALYZED" from all_tables 
                             group by tablespace_name) b
                             on a.tablespace_name = b.tablespace_name"""
    cur.execute(tablespace_stats_Query)
    for result in cur:
        tablespace_name = str(result[0])
        f.write("oracle.table_num[" + tablespace_name + "]|" + str(result[1]) + '|\n')
        f.write("oracle.last_analyzed[" + tablespace_name + "]|" + str(result[2]) + '|\n')
        f.write("oracle.earliest_analyzed[" + tablespace_name + "]|" + str(result[3]) + '|\n')
        f.write("oracle.maxrows[" + tablespace_name + "]|" + str(result[4]) + '|\n')
        f.write("oracle.maxblocks[" + tablespace_name + "]|" + str(result[5]) + '|\n')
       
    tablelist=[]
    table_reorg_Query = """select TABLE_NAME,status,num_rows,EMPTY_BLOCKS,BLOCKS,CHAIN_CNT from dba_tables where EMPTY_BLOCKS < BLOCKS * 0.8 
                         and CHAIN_CNT > 0.2*num_rows and rownum < 5"""
    cur.execute(table_reorg_Query)
    for result in cur:
        f.write("oracle.status[" + str(result[0]) + "]|" + str(result[1]) + '|\n')
        f.write("oracle.num_rows[" + str(result[0]) + "]|" + str(result[2]) + '|\n')
        f.write("oracle.empty_blocks[" + str(result[0]) + "]|" + str(result[3]) + '|\n')
        f.write("oracle.blocks[" + str(result[0]) + "]|" + str(result[4]) + '|\n')
        f.write("oracle.chain_cnt[" + str(result[0]) + "]|" + str(result[5]) + '|\n')
        tablelist += [{'{#TABNAME}':str(result[0])}]
    
    indexlist=[]
    index_reorg_Query = """select t.name, a.table_name , a.status,t.blocks, t.lf_rows,t.lf_rows - t.del_lf_rows as lf_rows_used ,
     to_char((t.del_lf_rows/t.lf_rows) * 100, '999.999') as ratio 
    from dba_indexes a,index_stats t where (t.del_lf_rows/t.lf_rows) * 100 > 15
    and a.index_name = t.name
    and rownum < 5"""
    cur.execute(index_reorg_Query)
    for result in cur:
        f.write("oracle.table_name[" + str(result[0]) + "]|" + str(result[1]) + '|\n')
        f.write("oracle.status[" + str(result[0]) + "]|" + str(result[2]) + '|\n')    
        f.write("oracle.index_blocks[" + str(result[0]) + "]|" + str(result[3]) + '|\n')
        f.write("oracle.index_lfrows[" + str(result[0]) + "]|" + str(result[4]) + '|\n')
        f.write("oracle.lf_rows_used[" + str(result[0]) + "]|" + str(result[5]) + '|\n')
        f.write("oracle.lf_ratio[" + str(result[0]) + "]|" + str(result[6]) + '|\n')
        indexlist += [{'{#INDEXNAME}':str(result[0])}]
        
    #invaild object
    objectlist=[]
    invaild_obj_Query = "select OBJECT_NAME,OBJECT_TYPE,CREATED,LAST_DDL_TIME,STATUS from dba_invalid_objects"
    cur.execute(invaild_obj_Query)
    for result in cur:
        f.write("oracle.obj_type[" + str(result[0]) + "]|" + str(result[1]) + '|\n')
        f.write("oracle.created[" + str(result[0]) + "]|" + str(result[2]) + '|\n')
        f.write("oracle.last_ddl_time[" + str(result[0]) + "]|" + str(result[3]) + '|\n')
        f.write("oracle.status[" + str(result[0]) + "]|" + str(result[4]) + '|\n')
        objectlist += [{'{#INVOBJNAME}':str(result[0])}]
    
    #alert diag
    alertlist=[]
    diag_Query = """select rownum,a.*
           from (
           select count(*),
           min(ORIGINATING_TIMESTAMP) "FRIST_TIMESTAMP",max(ORIGINATING_TIMESTAMP) "LAST_TIMESTAMP",COMPONENT_ID,MESSAGE_TYPE, 
           message_text "error"
           from 
           x$dbgalertext
           where originating_timestamp > (sysdate - 5/1440) 
             and message_text like '%ORA-%'
             group by COMPONENT_ID,MESSAGE_TYPE,message_text
             order by LAST_TIMESTAMP) a"""
    if (oramode=='SYSDBA' or oramode=='SYSOPER') :
        cur.execute(diag_Query)
        for result in cur:
            f.write("oracle.alert_time[" + str(result[0]) + "]|" + str(result[1]) + '|\n')
            f.write("oracle.alert_comp[" + str(result[0]) + "]|" + str(result[2]) + '|\n')
            f.write("oracle.alert_type[" + str(result[0]) + "]|" + str(result[3]) + '|\n')
            f.write("oracle.alertmsg[" + str(result[0]) + "]|" + str(result[4]) + '|\n')
            alertlist += [{'{#ALERTNUM}':str(result[0])}]
    else:
        print "Alert info Must be Collected by SYSDBA"

    tbspace_discovery = simplejson.dumps({'data':tbspacelist},sort_keys=True,separators=(',',':'))
    f.write("oracle_tbsp_discovery|" + str(tbspace_discovery) + '|\n')
    loggroup_discovery = simplejson.dumps({'data':loggrouplist},sort_keys=True,separators=(',',':'))
    f.write("oracle_loggroup_discovery|" + str(loggroup_discovery) + '|\n')
    table_discovery = simplejson.dumps({'data':tablelist},sort_keys=True,separators=(',',':'))
    f.write("oracle_table_discovery|" + str(table_discovery) + '|\n')
    index_discovery = simplejson.dumps({'data':indexlist},sort_keys=True,separators=(',',':'))
    f.write("oracle_index_discovery|" + str(index_discovery) + '|\n')
    invaild_discovery = simplejson.dumps({'data':objectlist},sort_keys=True,separators=(',',':'))
    f.write("oracle_invobj_discovery|" + str(invaild_discovery) + '|\n')
    alert_discovery = simplejson.dumps({'data':alertlist},sort_keys=True,separators=(',',':'))
    f.write("oracle_alertnum_discovery|" + str(alert_discovery) + '|\n')
    sql_discovery = simplejson.dumps({'data':sqllist},sort_keys=True,separators=(',',':'))
    f.write("oracle_sqlid_discovery|" + str(sql_discovery) + '|\n')
    
    cur.execute("truncate table DB_BASELINE")
    cur.execute("insert into DB_BASELINE (name,value) values ('snap time',(sysdate - to_date('01-01-1970','DD-MM-YYYY')) * (86400))")
    cur.execute("insert into DB_BASELINE select name,value from v$sysstat")
    
    con.commit()
    cur.close()
    con.close()

def checkOracle_mutli(checklist):
    #checkdb list
    try:
        f = open(checklist, "r")  
        while True:  
            line = f.readline()
            if line :
                if line[0] != '#' :  
                    checkOracle(line.split(' ')[0].strip(),line.split(' ')[1].strip(),line.split(' ')[2].strip(),line.split(' ')[3].strip(),line.split(' ')[4].strip())
            else:  
                break
    except Exception,e:
        s=sys.exc_info()
        print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)
    finally:
        f.close()
    
def main():  
    parser = OptionParser(usage=" %prog [-d database] [-U username] [-P password] [-I <interface or address>] [-p port][-a authentication] |\n\t%prog [-i <input file>]",version="%prog 1.1")

    parser.add_option("-d", "--database", action="store", 
                  type="string",
                  dest="db", 
                  help="specified Oracle SID ")
    parser.add_option("-U", "--user", action="store",
                  type="string",
                  dest="user",
                  help="specified connect User ")
    parser.add_option("-P", "--password", action="store",
                  type="string",
                  dest="password",
                  help="Password for connect User")     
    parser.add_option("-I", "--address", action="store",
                  type="string",
                  dest="address",
                  help="sperified the address for check database")
    parser.add_option("-p", "--port", action="store",
                  type="string",
                  dest="port",
                  help="sperified the port for check database")
    parser.add_option("-i", "--input", action="store", 
                  type="string",
                  dest="filename", 
                  help="sperified the file include connection infomation")          

    (options, args) = parser.parse_args() 

    function = 0
    if options.db and options.user and options.password and options.address and options.port and options.filename == None:
        function = 1
        checkOracle(options.db,options.user,options.password,options.address,options.port)
    elif options.db == None and options.user == None and options.password == None and options.address == None and options.port == None and options.filename:
        function = 2
        checkOracle_mutli(options.filename)
    else:
        function = -1
        parser.print_help()
       
if __name__ == "__main__":  
    main()
