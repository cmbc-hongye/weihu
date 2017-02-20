# -*- coding: utf-8 -*-
#!/usr/bin/env python
#####################################################################
#coding:utf-8                                                       #
#           Programe : check DB2 Database                           #
#           Author   : Hong Ye                                      #
#                      Ge Lin Feng                                  #
#           Python Versin : 2.7                                     #
#           Version :                                               #
#            1.0 : Initially Script   2016-07-27                    #
#            1.1 : Add Hidden Info    2016-07-30                    #
#            1.2 : Fix connect Bug    2016-07-31                    #
#            1.3 : Add Trigger Item   2016-08-11                    #
#            1.4 : Change Performance 2016-08-14                    #
#            1.5 : Fix DB2 Level Bug  2016-09-13                    #
#            1.6 : Fix agent          2016-09-19                    #
#            1.7 : Fix Diag /stmt     2016-09-21                    #
#            2.0 : Big Change         2016-11-12                    #
#            2.5 : support for DPF feature  2016-12-05              #
#            2.6 : support for multi-hosts  2016-12-07              #
#            2.7 : Fix dbsize with new SQL  2016-12-23              #
#            2.8 : add unavailable_Item function 2017-01-12         #
#            3.0 : Fix CodePage Problem 2017-02-17                  #
#####################################################################
import ibm_db
import json
import sys,datetime,re
import locale
from optparse import OptionParser
from multiprocessing import Process, Queue, current_process

query_dict={
             "Version_Query":"""
                              select INST_NAME,
                                      SERVICE_LEVEL
                              from sysibmadm.ENV_INST_INFO
                            """
            ,"DPF_PureScale_Query":"""
                                 select
                                      case  NUM_DBPARTITIONS
                                           when 1 then 'NON DPF'
                                           else 'DPF' end as IS_DPF ,
                                      case NUM_MEMBERS
                                           when 1 then 'NON Purescale'
                                           else 'Purescale' end as IS_PURESCALE
                                  from sysibmadm.ENV_INST_INFO
                                 """
            ,"DPF_Query":"""
                                 select
                                      case  NUM_DBPARTITIONS
                                           when 1 then 'NON DPF'
                                           else 'DPF' end as IS_DPF
                                  from sysibmadm.ENV_INST_INFO
                      """
            ,"which_nodes":"""
                               SELECT * FROM TABLE(DB_PARTITIONS()) as T
                           """
            ,"Dbstatus_Query":"""
                              select DB_PATH,DB_STATUS,
                                       COALESCE(LAST_BACKUP,'1900-01-01-00.00.00') as LAST_BACKUP,
                                       case when TIMESTAMPDIFF(16,CHAR(SNAPSHOT_TIMESTAMP-LAST_BACKUP)) is NULL then '999'
                                            else TIMESTAMPDIFF(16,CHAR(SNAPSHOT_TIMESTAMP-LAST_BACKUP))
                                        end as BACKUP_DIFF,
                                       DB_CONN_TIME,COALESCE(APPL_ID_OLDEST_XACT,0) as APPL_ID_OLDEST_XACT,
                                       DBPARTITIONNUM
                                from sysibmadm.snapdb
                              """
            ,"LICENSE_Query":"select LICENSE_INSTALLED from sysibmadm.ENV_PROD_INFO where INSTALLED_PROD_FULLNAME like '%_SERVER_%'"
            ,"DBSIZE_Query":"""select sum(DB_SIZE) as DB_SIZE from (
                             (select sum(TBSP_TOTAL_SIZE_KB)*1024 as DB_SIZE from sysibmadm.TBSP_UTILIZATION)
                             union all
                             (select TOTAL_LOG_AVAILABLE+TOTAL_LOG_USED as DB_SIZE from SYSIBMADM.SNAPDB))"""
            ,"Archlog_Query":"""
                               select case value when 'OFF' then 'CIRCLE LOGGING'
                                                  else 'ARCHIVAL LOGGING'
                                        end as LOGMODE,
                                        case value when 'OFF' then 0
                                                   else 1 end as ARCHIVELOG,
                                        DBPARTITIONNUM
                                from sysibmadm.dbcfg
                                where name='logarchmeth1'
                            """
            ,"Total_Memory_Query":"""
                                      select trim(char(agent_memory+db_memory+dbm_memory)) as total_memory
                                       from
                                            (select sum(POOL_CUR_SIZE) as agent_memory from sysibmadm.SNAPAGENT_MEMORY_POOL) a,
                                            (select sum(POOL_CUR_SIZE) as db_memory from sysibmadm.SNAPDB_MEMORY_POOL) b,
                                            (select sum(POOL_CUR_SIZE) as dbm_memory from sysibmadm.SNAPDBM_MEMORY_POOL) c
                                   """
            ,"Table_exits_Query":"select tabname from syscat.tables where tabname='DB_BASELINE'"
            ,"METRIC_DELTA_Query":"""
                                    CREATE FUNCTION METRIC_DELTA( CURR BIGINT, BASELINE BIGINT )
                                     RETURNS BIGINT
                                     LANGUAGE SQL
                                     CONTAINS SQL
                                    DETERMINISTIC
                                    RETURN CASE WHEN BASELINE IS NULL THEN CURR
                                                WHEN BASELINE <= CURR THEN CURR - BASELINE
                                                                      ELSE CURR + (9223368 - BASELINE)
                                     END
                                  """
            ,"Database_Baseline_Table":"CREATE TABLE DB_BASELINE LIKE sysibmadm.snapdb"
            ,"tbsp_Query":"""
                            select TBSP_NAME,TBSP_TYPE,
                                    case TBSP_STATE when 'NORMAL' then 1
                                                    else 255
                                    end as TBSP_STATUS,
                                    TBSP_STATE,
                                    TBSP_PAGE_SIZE,
                                    case when TBSP_USING_AUTO_STORAGE + TBSP_AUTO_RESIZE_ENABLED > 0 then 'AUTORESIZE'
                                         else 'NON-AUTORESIZE'
                                    end as TBSP_AUTO_RESIZE_ENABLED,
                                    TBSP_TOTAL_SIZE_KB * 1024 as TBSP_TOTAL_SIZE_KB,
                                    TBSP_TOTAL_SIZE_KB * TBSP_UTILIZATION_PERCENT/100 * 1024 as USED_KB,
                                    int(TBSP_UTILIZATION_PERCENT) as TBSP_UTILIZATION_PERCENT ,
                                    TBSP_EXTENT_SIZE,
                                    TBSP_PAGE_TOP*TBSP_PAGE_SIZE as TBSP_PAGE_TOP,
                                    case when TBSP_UTILIZATION_PERCENT > 85 and TBSP_TYPE = 'DMS' and TBSP_USING_AUTO_STORAGE + TBSP_AUTO_RESIZE_ENABLED = 0 then int(TBSP_UTILIZATION_PERCENT)
                                         else 10 end as TBSP_UTILIZATION_JUDGE,
                                    DBPARTITIONNUM
                             from sysibmadm.TBSP_UTILIZATION
                           """
            ,"Log_Query":"""
                            SELECT (TOTAL_LOG_AVAILABLE+TOTAL_LOG_USED) AS TRANLOG_TOTAL,
                                   TOTAL_LOG_USED AS TRANLOG_USED,100*TOTAL_LOG_USED/(TOTAL_LOG_AVAILABLE+TOTAL_LOG_USED) as LOG_USE_PERCENT,
                                   DBPARTITIONNUM
                            FROM SYSIBMADM.SNAPDB
                         """
            ,"REG_Query":"select REG_VAR_NAME,REG_VAR_VALUE,DBPARTITIONNUM from sysibmadm.REG_VARIABLES"
            ,"DBMCFG_Query":"""
                              select name,value
                              from sysibmadm.dbmcfg
                              where name in ('dft_mon_bufpool',
                                            'dft_mon_lock',
                                            'dft_mon_sort',
                                            'dft_mon_stmt',
                                            'dft_mon_table',
                                            'dft_mon_timestamp',
                                            'dft_mon_uow',
                                            'health_mon',
                                            'mon_heap_sz',
                                            'audit_buf_sz',
                                            'instance_memory',
                                            'sheapthres',
                                            'indexrec',
                                            'intra_parallel'
                                            )
                            """
            ,"DBCFG_Query":"""
                            select name,
                                   value,
                                   DBPARTITIONNUM
                            from sysibmadm.dbcfg
                            where name in('dft_degree','self_tuning_mem','database_memory','db_mem_thresh','locklist','maxlocks','sortheap',
                                          'dbheap','logbufsz','util_heap_sz','applheapsz','appl_memory','stat_heap_sz','dlchktime','locktimeout',
                                          'trackmod','maxfilop','logfilsiz','logprimary','logsecond','blk_log_dsk_ful','logarchmeth1','autorestart','indexrec','logindexbuild',
                                          'num_db_backups','rec_his_retentn','auto_del_rec_obj','auto_maint','auto_db_backup','auto_tbl_maint','auto_runstats','auto_stmt_stats',
                                          'auto_reorg'
                                         )
                            Union all
                            select name,
                                   case  value when '-1' then '(MAXAPPLS*5)'
                                              else value
                                              end as value,
                                    DBPARTITIONNUM
                            from sysibmadm.dbcfg
                            where name in ('pckcachesz','catalogcache_sz')
                           """

            ,"Memory_Query":"""
                               select a.BP_NAME as BP_NAME,
                                      dec(b.PAGESIZE/1024.0*b.npages/1024,17,2) as POOL_CUR_SIZE,
                                      dec(100 - 100*(POOL_DATA_P_READS + POOL_INDEX_P_READS + POOL_XDA_P_READS)/(POOL_DATA_L_READS+POOL_INDEX_L_READS+POOL_XDA_L_READS+1),17,2) as BP_HITRATIO,
                                      a.DBPARTITIONNUM as DBPARTITIONNUM
                               from sysibmadm.SNAPBP a ,syscat.bufferpools b
                               where a.BP_NAME=b.BPNAME and a.BP_NAME not like 'IBMSYSTEMBP%'
                               union all
                               select bb.POOL_ID as BP_NAME,
                                      dec(bb.POOL_CUR_SIZE/1024.0/1024,17,2) as POOL_CUR_SIZE,
                                     dec(100 - 100.0*aa.PKG_CACHE_INSERTS/(aa.PKG_CACHE_LOOKUPS+1),17,2) as BP_HITRATIO,
                                     aa.DBPARTITIONNUM
                               from sysibmadm.SNAPDB_MEMORY_POOL bb, SYSIBMADM.SNAPDB aa
                               where bb.POOL_ID = 'PACKAGE_CACHE' and aa.DBPARTITIONNUM=bb.DBPARTITIONNUM
                               union all
                               select cc.POOL_ID as BP_NAME,
                                      dec(cc.POOL_CUR_SIZE/1024.0/1024,17,2) as POOL_CUR_SIZE,
                                      dec(100 - 100.0*dd.CAT_CACHE_INSERTS/(dd.CAT_CACHE_LOOKUPS+1),17,2) as BP_HITRATIO,
                                      cc.DBPARTITIONNUM
                               from sysibmadm.SNAPDB_MEMORY_POOL cc,SYSIBMADM.SNAPDB dd
                               where POOL_ID = 'CAT_CACHE' and cc.DBPARTITIONNUM=dd.DBPARTITIONNUM
                               union all
                               select ee.POOL_ID as BP_NAME,
                                      dec(ee.POOL_CUR_SIZE/1024.0/1024,17,2) as POOL_CUR_SIZE,
                                      dec(100 - 100.0*ff.APPL_SECTION_INSERTS/(ff.APPL_SECTION_LOOKUPS+1),17,2) as BP_HITRATIO,
                                      ee.DBPARTITIONNUM
                               from sysibmadm.SNAPDB_MEMORY_POOL ee,SYSIBMADM.SNAPDB ff
                               where POOL_ID = 'APPL_SHARED' and ee.DBPARTITIONNUM=ff.DBPARTITIONNUM
                            """
            ,"dbsnap_Query":"""
                              select
                                  COALESCE(B.SNAPSHOT_TIMESTAMP,A.DB_CONN_TIME) as START_TIMESTAMP,
                                  A.SNAPSHOT_TIMESTAMP as END_TIMESTAMP,
                                  TIMESTAMPDIFF(2,CHAR(A.SNAPSHOT_TIMESTAMP-COALESCE(B.SNAPSHOT_TIMESTAMP,A.DB_CONN_TIME))) as REAL_TIME,
                                  METRIC_DELTA(A.ELAPSED_EXEC_TIME_S, B.ELAPSED_EXEC_TIME_S) as ELAPSED_EXEC_TIME_S,
                                  METRIC_DELTA(A.ELAPSED_EXEC_TIME_S, B.ELAPSED_EXEC_TIME_S)/(METRIC_DELTA(A.COMMIT_SQL_STMTS, B.COMMIT_SQL_STMTS) + METRIC_DELTA(A.ROLLBACK_SQL_STMTS, B.ROLLBACK_SQL_STMTS)+1) as AVG_TRANS_TIME,
                                  METRIC_DELTA(A.ELAPSED_EXEC_TIME_S, B.ELAPSED_EXEC_TIME_S)/(METRIC_DELTA(A.SELECT_SQL_STMTS, B.SELECT_SQL_STMTS) + METRIC_DELTA(A.UID_SQL_STMTS, B.UID_SQL_STMTS) + 1) as AVG_SQL_TIME,
                                  (METRIC_DELTA(A.COMMIT_SQL_STMTS, B.COMMIT_SQL_STMTS) + METRIC_DELTA(A.ROLLBACK_SQL_STMTS, B.ROLLBACK_SQL_STMTS))/(TIMESTAMPDIFF(4,CHAR(A.SNAPSHOT_TIMESTAMP-COALESCE(B.SNAPSHOT_TIMESTAMP,A.DB_CONN_TIME)))+1) AS TPM,
                                  100*METRIC_DELTA(A.COMMIT_SQL_STMTS, B.COMMIT_SQL_STMTS)/(METRIC_DELTA(A.COMMIT_SQL_STMTS, B.COMMIT_SQL_STMTS) + METRIC_DELTA(A.ROLLBACK_SQL_STMTS, B.ROLLBACK_SQL_STMTS)+1) AS SUCCESS_TRANS_RATIO,
                                  (METRIC_DELTA(A.SELECT_SQL_STMTS, B.SELECT_SQL_STMTS) + METRIC_DELTA(A.UID_SQL_STMTS, B.UID_SQL_STMTS))/(METRIC_DELTA(A.COMMIT_SQL_STMTS, B.COMMIT_SQL_STMTS) + METRIC_DELTA(A.ROLLBACK_SQL_STMTS, B.ROLLBACK_SQL_STMTS)+1) AS QUERY_PER_TRANS,
                                  METRIC_DELTA(A.ROWS_SELECTED, B.ROWS_SELECTED)/(METRIC_DELTA(A.ROWS_DELETED, B.ROWS_DELETED) + METRIC_DELTA(A.rows_inserted, B.rows_inserted) + METRIC_DELTA(A.rows_updated, B.rows_updated)+1) as READ_VS_WRITE,
                                  (METRIC_DELTA(A.ROWS_DELETED, B.ROWS_DELETED) + METRIC_DELTA(A.rows_inserted, B.rows_inserted) + METRIC_DELTA(A.rows_updated, B.rows_updated))/(METRIC_DELTA(A.COMMIT_SQL_STMTS, B.COMMIT_SQL_STMTS) + METRIC_DELTA(A.ROLLBACK_SQL_STMTS, B.ROLLBACK_SQL_STMTS) + 1) as MODIFIED_ROWS_PER_TRANS,
                                  METRIC_DELTA(A.ROWS_READ, B.ROWS_READ)/(METRIC_DELTA(A.SELECT_SQL_STMTS, B.SELECT_SQL_STMTS) + METRIC_DELTA(A.UID_SQL_STMTS, B.UID_SQL_STMTS) + 1) as ROWS_READ_PER_SQL,
                                  METRIC_DELTA(A.TOTAL_SORT_TIME, B.TOTAL_SORT_TIME)/1000 as TOTAL_SORT_TIME_S,
                                  METRIC_DELTA(A.LOCK_WAIT_TIME, B.LOCK_WAIT_TIME)/1000 as TOTAL_LOCK_WAIT_TIME_S,
                                  METRIC_DELTA(A.POOL_READ_TIME, B.POOL_READ_TIME)/1000 as POOL_READ_TIME_S,
                                  METRIC_DELTA(A.POOL_ASYNC_READ_TIME, B.POOL_ASYNC_READ_TIME)/1000 as POOL_ASYNC_READ_TIME_S,
                                  METRIC_DELTA(A.POOL_WRITE_TIME, B.POOL_WRITE_TIME)/1000 as POOL_WRITE_TIME_S,
                                  METRIC_DELTA(A.POOL_ASYNC_WRITE_TIME, B.POOL_ASYNC_WRITE_TIME)/1000 as POOL_ASYNC_WRITE_TIME_S,
                                  METRIC_DELTA(A.DIRECT_READ_TIME, B.DIRECT_READ_TIME)/1000 as DIRECT_READ_TIME_S,
                                  METRIC_DELTA(A.DIRECT_WRITE_TIME, B.DIRECT_WRITE_TIME)/1000 as DIRECT_WRITE_TIME_S,
                                  METRIC_DELTA(A.PREFETCH_WAIT_TIME, B.PREFETCH_WAIT_TIME)/1000 as TOTAL_PREFETCH_WAIT_TIME_S,
                                  METRIC_DELTA(A.LOG_READ_TIME_S, B.LOG_READ_TIME_S) as LOG_READ_TIME_S,
                                  METRIC_DELTA(A.LOG_WRITE_TIME_S, B.LOG_WRITE_TIME_S) as LOG_WRITE_TIME_S,
                                  METRIC_DELTA(A.TOTAL_CONS, B.TOTAL_CONS)/(TIMESTAMPDIFF(4,CHAR(A.SNAPSHOT_TIMESTAMP-COALESCE(B.SNAPSHOT_TIMESTAMP,A.DB_CONN_TIME)))+1) as AVG_CONS,
                                  A.APPLS_CUR_CONS,
                                  A.NUM_ASSOC_AGENTS as CUR_AGENT,
                                  METRIC_DELTA(A.SORT_OVERFLOWS, B.SORT_OVERFLOWS)/(METRIC_DELTA(A.TOTAL_SORTS, B.TOTAL_SORTS)+1) as SORT_OVERFLOWS_PERCENT,
                                  METRIC_DELTA(A.TOTAL_SORT_TIME, B.TOTAL_SORT_TIME)/(METRIC_DELTA(A.TOTAL_SORTS, B.TOTAL_SORTS)+1) as AVG_SORT_TIME,
                                  METRIC_DELTA(A.TOTAL_SORTS, B.TOTAL_SORTS)/(METRIC_DELTA(A.SELECT_SQL_STMTS, B.SELECT_SQL_STMTS) + METRIC_DELTA(A.UID_SQL_STMTS, B.UID_SQL_STMTS) + 1) as SORT_PERCENT,
                                  METRIC_DELTA(A.LOCK_WAITS, B.LOCK_WAITS) as LOCK_WAITS,
                                  METRIC_DELTA(A.DEADLOCKS, B.DEADLOCKS) as DEADLOCKS,
                                  METRIC_DELTA(A.LOCK_ESCALS, B.LOCK_ESCALS) as LOCK_ESCALS,
                                  METRIC_DELTA(A.LOCK_TIMEOUTS, B.LOCK_TIMEOUTS) as LOCK_TIMEOUTS,
                                  METRIC_DELTA(A.LOCK_WAIT_TIME, B.LOCK_WAIT_TIME)/(1000*METRIC_DELTA(A.LOCK_WAITS, B.LOCK_WAITS)+1) as AVG_LOCK_WAIT_TIME,
                                  METRIC_DELTA(A.rows_read, B.rows_read)/(METRIC_DELTA(A.ROWS_SELECTED, B.ROWS_SELECTED)+1) as SELECTPERREAD,
                                  100 - 100*METRIC_DELTA(A.POOL_ASYNC_DATA_READS, B.POOL_ASYNC_DATA_READS)/(METRIC_DELTA(A.POOL_DATA_P_READS, B.POOL_DATA_P_READS) + METRIC_DELTA(A.POOL_TEMP_DATA_P_READS, B.POOL_TEMP_DATA_P_READS) + 1) as DATA_SYNC_READ_PERCENT,
                                  100 - 100*METRIC_DELTA(A.POOL_ASYNC_DATA_WRITES, B.POOL_ASYNC_DATA_WRITES)/(METRIC_DELTA(A.POOL_DATA_WRITES, B.POOL_DATA_WRITES) +1) as DATA_SYNC_WRITE_PERCENT,
                                  100*METRIC_DELTA(A.UNREAD_PREFETCH_PAGES, B.UNREAD_PREFETCH_PAGES)/(METRIC_DELTA(A.POOL_ASYNC_DATA_READS, B.POOL_ASYNC_DATA_READS) + METRIC_DELTA(A.POOL_ASYNC_INDEX_READS, B.POOL_ASYNC_INDEX_READS) + 1) as UNREAD_PREFETCH_PERCENT,
                                  100 - 100*METRIC_DELTA(A.PREFETCH_WAIT_TIME, B.PREFETCH_WAIT_TIME)/(METRIC_DELTA(A.POOL_ASYNC_READ_TIME, B.POOL_ASYNC_READ_TIME) + 1) as PREFETCH_WAIT_TIME_PERCENT,
                                  METRIC_DELTA(A.POOL_ASYNC_DATA_READS, B.POOL_ASYNC_DATA_READS)/(METRIC_DELTA(A.POOL_ASYNC_DATA_READ_REQS, B.POOL_ASYNC_DATA_READ_REQS) + 1) as AVG_ASYNC_DATA_PAGE,
                                  METRIC_DELTA(A.POOL_ASYNC_INDEX_READ_REQS, B.POOL_ASYNC_INDEX_READ_REQS)/(METRIC_DELTA(A.POOL_ASYNC_INDEX_READS, B.POOL_ASYNC_INDEX_READS) + 1) as AVG_ASYNC_INDEX_PAGE,
                                  100 - 100 *METRIC_DELTA(A.NUM_LOG_PART_PAGE_IO, B.NUM_LOG_PART_PAGE_IO)/(METRIC_DELTA(A.NUM_LOG_WRITE_IO, B.NUM_LOG_WRITE_IO) +1) as LOG_PART_RATIO,
                                  (METRIC_DELTA(A.POOL_READ_TIME, B.POOL_READ_TIME) - METRIC_DELTA(A.POOL_ASYNC_READ_TIME, B.POOL_ASYNC_READ_TIME))/(METRIC_DELTA(A.POOL_DATA_P_READS, B.POOL_DATA_P_READS) + METRIC_DELTA(A.POOL_TEMP_DATA_P_READS, B.POOL_TEMP_DATA_P_READS) + METRIC_DELTA(A.POOL_INDEX_P_READS, B.POOL_INDEX_P_READS) + METRIC_DELTA(A.POOL_TEMP_INDEX_P_READS, B.POOL_TEMP_INDEX_P_READS) - METRIC_DELTA(A.POOL_ASYNC_DATA_READS, B.POOL_ASYNC_DATA_READS) - METRIC_DELTA(A.POOL_ASYNC_INDEX_READS, B.POOL_ASYNC_INDEX_READS) + 1) as AVG_SYNC_READ_TIME,
                                  METRIC_DELTA(A.POOL_ASYNC_READ_TIME, B.POOL_ASYNC_READ_TIME)/(METRIC_DELTA(A.POOL_ASYNC_DATA_READS, B.POOL_ASYNC_DATA_READS) + METRIC_DELTA(A.POOL_ASYNC_INDEX_READS, B.POOL_ASYNC_INDEX_READS)+ 1) AS AVG_ASYNC_READ_TIME,
                                  (METRIC_DELTA(A.POOL_WRITE_TIME, B.POOL_WRITE_TIME) - METRIC_DELTA(A.POOL_ASYNC_WRITE_TIME, B.POOL_ASYNC_WRITE_TIME))/(METRIC_DELTA(A.POOL_DATA_WRITES, B.POOL_DATA_WRITES) + METRIC_DELTA(A.POOL_INDEX_WRITES, B.POOL_INDEX_WRITES) - METRIC_DELTA(A.POOL_ASYNC_DATA_WRITES, B.POOL_ASYNC_DATA_WRITES) - METRIC_DELTA(A.POOL_ASYNC_INDEX_WRITES, B.POOL_ASYNC_INDEX_WRITES) +1) as SYNC_POOL_WRITE_TIME,
                                  METRIC_DELTA(A.POOL_ASYNC_WRITE_TIME, B.POOL_ASYNC_WRITE_TIME)/(METRIC_DELTA(A.POOL_ASYNC_DATA_WRITES, B.POOL_ASYNC_DATA_WRITES) + METRIC_DELTA(A.POOL_ASYNC_INDEX_WRITES, B.POOL_ASYNC_INDEX_WRITES) +1) as ASYNC_POOL_WRITE_TIME,
                                  METRIC_DELTA(A.DIRECT_READ_TIME, B.DIRECT_READ_TIME)/(METRIC_DELTA(A.DIRECT_READS, B.DIRECT_READS) + 1) AS AVG_DIRECT_READ_TIME,
                                  METRIC_DELTA(A.DIRECT_WRITE_TIME, B.DIRECT_WRITE_TIME)/(METRIC_DELTA(A.DIRECT_WRITES, B.DIRECT_WRITES) + 1) AS AVG_DIRECT_WRITE_TIME,
                                  METRIC_DELTA(A.LOG_READ_TIME_S, B.LOG_READ_TIME_S)*1000.0/(METRIC_DELTA(A.LOG_READS, B.LOG_READS) + 1) as AVG_LOG_READ_TIME,
                                  METRIC_DELTA(A.LOG_WRITE_TIME_S, B.LOG_WRITE_TIME_S)*1000.0/(METRIC_DELTA(A.LOG_WRITES, B.LOG_WRITES) + 1) as AVG_LOG_WRITE_TIME,
                                  METRIC_DELTA(A.COMMIT_SQL_STMTS,B.COMMIT_SQL_STMTS) as DB_COMMIT_SQL_STMTS,
                                  METRIC_DELTA(A.CONNECTIONS_TOP,B.CONNECTIONS_TOP) as DB_CONN_HWM,
                                  (100 - 100*METRIC_DELTA(A.PRIV_WORKSPACE_SECTION_LOOKUPS, B.PRIV_WORKSPACE_SECTION_LOOKUPS)/(METRIC_DELTA(A.PRIV_WORKSPACE_SECTION_INSERTS, B.PRIV_WORKSPACE_SECTION_INSERTS) + 1)) as PRIV_MEMORY_HITRATIO,
                                  (100 - 100*METRIC_DELTA(A.SHR_WORKSPACE_SECTION_LOOKUPS, B.SHR_WORKSPACE_SECTION_LOOKUPS)/(METRIC_DELTA(A.SHR_WORKSPACE_SECTION_INSERTS, B.SHR_WORKSPACE_SECTION_INSERTS) + 1)) as SHR_MEMORY_HITRATIO,
                                  A.DBPARTITIONNUM
                                  FROM SYSIBMADM.SNAPDB A LEFT JOIN DB_BASELINE B ON A.DB_NAME=B.DB_NAME where A.DBPARTITIONNUM=B.DBPARTITIONNUM fetch first 1 rows only
                            """

            ,"Agent_Query":"""
                              select max_appl,max_agent,DBPARTITIONNUM
                              from (select value as max_appl,DBPARTITIONNUM from sysibmadm.dbcfg where name='maxappls') a,
                                   (select value as max_agent from sysibmadm.dbmcfg where name='max_coordagents') b
                           """
            ,"TopSQL_Query":"""
                               select ROW_NUMBER() OVER (ORDER BY NUM_EXECUTIONS) AS NUMBER ,APPLICATION_HANDLE,ACTIVITY_STATE,APPLICATION_NAME,a.NUM_EXECUTIONS,a.ROWS_READ,a.TOTAL_EXEC_TIME,
                               (a.TOTAL_USR_CPU_TIME*1000+a.TOTAL_USR_CPU_TIME_MS+a.TOTAL_SYS_CPU_TIME*1000+TOTAL_SYS_CPU_TIME_MS)/1000.0 as TOTAL_CPU_TIME,
                               a.STMT_TEXT,a.DBPARTITIONNUM
                               from sysibmadm.SNAPDYN_SQL a left join sysibmadm.MON_CURRENT_SQL b on a.STMT_TEXT=b.STMT_TEXT
                            """
            ,"unavailable_Item":"""
                                select TRIGSCHEMA,TRIGNAME,'TRIGGER' as TYPE,valid,CREATE_TIME,LAST_REGEN_TIME as ALTER_TIME from syscat.triggers where valid in ('N','X')
                                 union all
                                select ROUTINESCHEMA,ROUTINENAME,'ROUTINE' as TYPE,valid,CREATE_TIME,ALTER_TIME from syscat.routines where valid in ('N','X')
                                 union all
                                select a.VIEWSCHEMA,a.VIEWNAME,'VIEW' as TYPE,a.valid,b.CREATE_TIME,b.ALTER_TIME from syscat.views a,syscat.tables b where a.VIEWNAME=b.tabname and a.valid in ('N','X')
                                 union all
                                select TYPESCHEMA,TYPENAME,'DATATYPE' as TYPE,valid,CREATE_TIME,ALTER_TIME from syscat.datatypes where valid in ('N','X')
                                 union all
                                select VARSCHEMA,VARNAME,'VARIABLE' as TYPE,valid,CREATE_TIME,LAST_REGEN_TIME as ALTER_TIME from syscat.variables where valid in ('N','X')
                              """
            ,"Invaild_Query":"select OBJECTSCHEMA, OBJECTMODULENAME, OBJECTNAME, ROUTINENAME, OBJECTTYPE  from syscat.INVALIDOBJECTS"
            ,"Stats_Query":"""
                               SELECT a.TBSPACE as TABLESPACE,
                                      count(1) NO_OF_OBJ,
                                      COALESCE(max(max(b.stats_time),max(c.stats_time)),'1970-01-01-00.00.00.000000') as LATEST_STATS_TIME,
                                      COALESCE(min(min(b.stats_time),min(c.stats_time)),'1970-01-01-00.00.00.000000') as OLDEST_STATS_TIME,
                                      COALESCE(max(max(npages),max(NLEAF-NUM_EMPTY_LEAFS))+1,0) as LARGEST_NPAGES,
                                      COALESCE(max(max(fpages),max(NLEAF))+1,0) as LARGEST_FPAGES,
                                      COALESCE(max(max(card),max(INDCARD))+1,0) as LARGEST_CARD
                               FROM  syscat.tablespaces a
                                    left join syscat.tables b on a.tbspaceid=b.tbspaceid
                                    left join syscat.indexes c on a.tbspaceid=c.tbspaceid
                               GROUP BY a.TBSPACE
                            """
            ,"Table_Query": """
                               SELECT trim(t.tabschema) ||'.'|| trim(t.tabname) as TABLE,
                                      AVAILABLE ,
                                      LOAD_STATUS,
                                      ROWS_READ,
                                      ROWS_READ*1.0/(data_object_l_size + 1) AS TABLE_SCANS ,
                                      OVERFLOW_ACCESSES,
                                      PAGE_REORGS,dec(data_object_p_size*1.0 / (data_object_l_size + 1),10,2) as TABLE_USAGE,
                                      s.DBPARTITIONNUM
                               FROM sysibmadm.snaptab t ,
                                    SYSIBMADM.ADMINTABINFO s
                               where s.tabname=t.tabname
                                 and s.tabschema = t.tabschema
                                 and s.DBPARTITIONNUM=t.DBPARTITIONNUM
                                 and TAB_TYPE <> 'CATALOG_TABLE'
                                 and (rows_read > 10000000 or OVERFLOW_ACCESSES > 100000 or PAGE_REORGS > 20000 or AVAILABLE='N' or LOAD_STATUS = 'PENDING')
                            """
            ,"Index_Query":"""
                                SELECT trim(S.INDSCHEMA) || '.' || trim(S.INDNAME) AS INDNAME,
                                    T.INDEX_SCANS,
                                    ROOT_NODE_SPLITS + BOUNDARY_LEAF_NODE_SPLITS + NONBOUNDARY_LEAF_NODE_SPLITS as SPLITS,
                                    PSEUDO_EMPTY_PAGES,
                                    PSEUDO_DELETES
                                FROM TABLE(MON_GET_INDEX('','', -2)) as T,
                                     SYSCAT.INDEXES AS S
                                WHERE T.TABSCHEMA = S.TABSCHEMA AND
                                      T.TABNAME = S.TABNAME AND
                                      T.IID = S.IID  AND
                                      (PSEUDO_EMPTY_PAGES + PSEUDO_DELETES > 10000 or ROOT_NODE_SPLITS + BOUNDARY_LEAF_NODE_SPLITS + NONBOUNDARY_LEAF_NODE_SPLITS > 10000)
                            """
             ,"diag_Query":"""
                            select ROW_NUMBER() OVER (ORDER BY max(timestamp) DESC) AS ROW_NUMBER,
                                    min(timestamp) as START_TIME,max(timestamp) as END_TIME,
                                    count(*) as COUNT,
                                    LEVEL,
                                    case when dbname is null then 'NULL' else dbname end as dbname,
                                    case when AUTH_ID is null then 'NULL' else AUTH_ID end as auth_id,
                                    FUNCTION,
                                    trim(MSG) as MSG
                            from
                               (select timestamp,
                                       LEVEL,
                                       dbname,
                                       AUTH_ID,
                                       FUNCTION,
                                       VARCHAR(substr(replace(MSG,CHR(13)||CHR(10),' '),1,1024)) as MSG
                                from TABLE(PD_GET_DIAG_HIST('MAIN', '', '', Current timestamp - 7 days, current timestamp))
                                WHERE LEVEL IN ('C','E','S') and
                                  MSG is not null and
                                  FUNCTION is not null and
                                  MSG not like 'ZRC=0x870F003E=-2029060034%') a
                            group by dbname,LEVEL,AUTH_ID,FUNCTION,MSG
                            """
            ,
            }


class connDB():
    def __init__(self,dbname,username,password,ip='127.0.0.1',port=50000,authentication='SERVER'):
        self.dbname=dbname
        self.hostname=ip
        self.username=username
        self.password=password
        self.port=port
        self.authentication=authentication
    def establishConn(self):
        try :
            locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
            conn_config="DATABASE=%s;HOSTNAME=%s;PORT=%d;PROTOCOL=TCPIP;UID=%s;PWD=%s;AUTHENTICATION=%s;" %(self.dbname, self.hostname, int(self.port), self.username, self.password,self.authentication)
            self.conn=ibm_db.connect(conn_config,'','',{},ibm_db.QUOTED_LITERAL_REPLACEMENT_OFF)

            if self.conn is not None :
                if (ibm_db.server_info(self.conn).DB_CODEPAGE == 1208)  :
                    #print("APPL_CODEPAGE: int(%s)" % ibm_db.client_info(self.conn).APPL_CODEPAGE)
                    #print("CONN_CODEPAGE: int(%s)" % ibm_db.client_info(self.conn).CONN_CODEPAGE)
                    #print("DB_CODEPAGE: int(%s)" % ibm_db.server_info(self.conn).DB_CODEPAGE)
                    return self.conn
                elif (ibm_db.server_info(self.conn).DB_CODEPAGE == 1386 ):
                    ibm_db.close(self.conn)
                    locale.setlocale(locale.LC_ALL, 'zh_CN.GBK')
                    self.conn=ibm_db.connect(conn_config,'','',{},ibm_db.QUOTED_LITERAL_REPLACEMENT_ON)
                    #print("APPL_CODEPAGE: int(%s)" % ibm_db.client_info(self.conn).APPL_CODEPAGE)
                    #print("CONN_CODEPAGE: int(%s)" % ibm_db.client_info(self.conn).CONN_CODEPAGE)
                    #print("DB_CODEPAGE: int(%s)" % ibm_db.server_info(self.conn).DB_CODEPAGE)
                    return self.conn
                else :
                    print "Warning !!! Database Codepage cannot Recognize!!!"
                    print("APPL_CODEPAGE: int(%s)" % ibm_db.client_info(self.conn).APPL_CODEPAGE)
                    print("CONN_CODEPAGE: int(%s)" % ibm_db.client_info(self.conn).CONN_CODEPAGE)
                    print("DB_CODEPAGE: int(%s)" % ibm_db.server_info(self.conn).DB_CODEPAGE)
                    return self.conn
            else:
                print "connection failed for some reason."
                sys.exit(1)
        except Exception,e:
            print e

class checkDB():
    def __init__(self,connDB):
        self.connDB=connDB
        self.conn=self.connDB.establishConn()
        self.archetecure=self.what_is_the_archetecure()
        self.collect_basic_info()
        nodes_num_stmt=ibm_db.exec_immediate(self.conn,"SELECT count(*) as node_num FROM TABLE(DB_PARTITIONS()) as T")
        ibm_db.fetch_row(nodes_num_stmt)
        self.NODENUM=ibm_db.result(nodes_num_stmt,"NODE_NUM")
        self.listForoutput=[[] for i in range(self.NODENUM)]
        if self.archetecure=='DPF' or self.archetecure=='Purescale':
            for num in range(0,self.NODENUM):
                delimete_filed='='*15+'PARTITION-'+str(num)+'='*15
                self.listForoutput[num].append(delimete_filed)
        self.discoList=[[] for i in range(self.NODENUM)]

    def write_result_to_file(self):
        now=datetime.datetime.now()
        otherStyleTime=now.strftime("%Y-%m-%d-%H.%M.%S")
        filename="DB2_%s_%s_%d_%s.txt" %(self.connDB.hostname,self.connDB.dbname,self.NODENUM,str(otherStyleTime))
        fout=open(filename,'wt')
        try:
            for node_num in range(0,self.NODENUM):
               self.listForoutput[node_num]+=self.discoList[node_num]
               for item in self.listForoutput[node_num]:
                   fout.write(item+"\n")
        except Exception,e:
            print e
            sys.exit(1)
        fout.close()

    def what_is_the_archetecure(self):
        try:
            DB2_PRODUCT_VERSION_SET=set(['v9.8','v10','v11'])
            version_stmt = ibm_db.exec_immediate(self.conn, query_dict['Version_Query'])
            ibm_db.fetch_row(version_stmt)
            global instname
            global version
            instname = ibm_db.result(version_stmt, "INST_NAME")
            version = ibm_db.result(version_stmt, "SERVICE_LEVEL")

            if instname != self.connDB.username :
                print "Warning ! Recommand To Excute Checkdb2 Script by DB2 Instance User! "

            sub_version=version.split()[1].split(".")
            if sub_version[0] in DB2_PRODUCT_VERSION_SET or (sub_version[0]+"."+sub_version[1]) in DB2_PRODUCT_VERSION_SET:
                DPF_Purescale_stmt=ibm_db.exec_immediate(self.conn,query_dict['DPF_PureScale_Query'])
                ibm_db.fetch_row(DPF_Purescale_stmt)
                IS_DPF = ibm_db.result(DPF_Purescale_stmt, "IS_DPF")
                IS_PURESCALE = ibm_db.result(DPF_Purescale_stmt, "IS_PURESCALE")
            else:
                DPF_stmt=ibm_db.exec_immediate(self.conn,query_dict['DPF_Query'])
                ibm_db.fetch_row(DPF_stmt)
                IS_DPF = ibm_db.result(DPF_stmt, "IS_DPF")
                IS_PURESCALE="Not Supported"
            if IS_DPF =='DPF' :
                return 'DPF'
            elif IS_PURESCALE=='Purescale':
                return 'Purescale'
            else:
                return 'Standalone'
        except Exception,e:
            print e

    def collect_basic_info(self):
        try:
            Table_exists_stmt=ibm_db.exec_immediate(self.conn,query_dict['Table_exits_Query'])
            ibm_db.fetch_row(Table_exists_stmt)
            #tab_exits = ibm_db.result(Table_exits_stmt, "TABNAME")
            tab_exits=ibm_db.result(Table_exists_stmt,"TABNAME")
            if tab_exits is False :
                METRIC_DELTA_stmt = ibm_db.exec_immediate(self.conn, query_dict['METRIC_DELTA_Query'])
                Database_Baseline_stmt = ibm_db.exec_immediate(self.conn, query_dict["Database_Baseline_Table"])
        except Exception,e:
            print e

    def collect_db_info(self):
        DB2_PRODUCT_VERSION_SET=set(['v9.8','v10','v11'])
        try:
            version_stmt=ibm_db.exec_immediate(self.conn,query_dict['Version_Query'])
            ibm_db.fetch_row(version_stmt)
            instname = ibm_db.result(version_stmt, "INST_NAME")
            version = ibm_db.result(version_stmt, "SERVICE_LEVEL")
            self.listForoutput[0].append("db2.instname|" + instname.upper() + '|')
            self.listForoutput[0].append("db2.level|" + version + '|')
            self.listForoutput[0].append("db2.fixpack|" + version.split('.')[-1] + '|')
            self.listForoutput[0].append("db2.dbname|" + self.connDB.dbname.upper() + '|')

            archlog_stmt=ibm_db.exec_immediate(self.conn,query_dict['Archlog_Query'])
            archlog_result_dict=ibm_db.fetch_assoc(archlog_stmt)
            while archlog_result_dict !=False:
                logmode = archlog_result_dict["LOGMODE"]
                archivelog = archlog_result_dict["ARCHIVELOG"]
                node_num=archlog_result_dict["DBPARTITIONNUM"]

                self.listForoutput[node_num].append("db2.logmode|"+ str(logmode) + '|')
                self.listForoutput[node_num].append("db2.archivelog|"+str(archivelog) + '|')
                archlog_result_dict=ibm_db.fetch_assoc(archlog_stmt)

            Total_Memory_stmt = ibm_db.exec_immediate(self.conn, query_dict['Total_Memory_Query'])
            ibm_db.fetch_row(Total_Memory_stmt)
            total_memory = ibm_db.result(Total_Memory_stmt, "TOTAL_MEMORY")
            self.listForoutput[0].append("db2.total_memory|"+str(total_memory)+"|")

            Dbstatus_stmt = ibm_db.exec_immediate(self.conn, query_dict['Dbstatus_Query'])
            Dbstatus_result_dict=ibm_db.fetch_assoc(Dbstatus_stmt)
            while Dbstatus_result_dict is not False:
                uptime = Dbstatus_result_dict["DB_CONN_TIME"]
                backup_time = Dbstatus_result_dict["LAST_BACKUP"]
                backup_diff = Dbstatus_result_dict["BACKUP_DIFF"]
                db_status = Dbstatus_result_dict["DB_STATUS"]
                db_path = Dbstatus_result_dict["DB_PATH"]
                oldest_app = Dbstatus_result_dict["APPL_ID_OLDEST_XACT"]
                node_num=Dbstatus_result_dict["DBPARTITIONNUM"]

                self.listForoutput[node_num].append("db2.uptime|"+str(uptime.strftime("%Y-%m-%d %H:%M:%S")) + '|')
                self.listForoutput[node_num].append("db2.backup_time|" +  str(backup_time.strftime("%Y-%m-%d %H:%M:%S")) + '|')
                self.listForoutput[node_num].append("db2.backup_diff|" + str(backup_diff) + '|')
                self.listForoutput[node_num].append("db2.status|" + str(db_status) + '|')
                self.listForoutput[node_num].append("db2.path|" + db_path + '|')
                self.listForoutput[node_num].append("db2.oldest_app|"+str(oldest_app)+'|')
                Dbstatus_result_dict=ibm_db.fetch_assoc(Dbstatus_stmt)

            license_stmt = ibm_db.exec_immediate(self.conn, query_dict['LICENSE_Query'])
            license_result_dict=ibm_db.fetch_assoc(license_stmt)
            while license_result_dict is not False:
                lic = license_result_dict["LICENSE_INSTALLED"]
                self.listForoutput[0].append("db2.license|" + str(lic) + '|')
                license_result_dict=ibm_db.fetch_assoc(license_stmt)

            dbsize_stmt = ibm_db.exec_immediate(self.conn, query_dict['DBSIZE_Query'])
            dbsize_result_dict=ibm_db.fetch_assoc(dbsize_stmt)
            while dbsize_result_dict is not False:
                dbsize = dbsize_result_dict["DB_SIZE"]
                self.listForoutput[0].append("db2.dbsize|" + str(dbsize) + '|')
                dbsize_result_dict=ibm_db.fetch_assoc(dbsize_stmt)

        except Exception,e:
            print e
    def collect_capacity_info(self):
        tbsp_stmt = ibm_db.exec_immediate(self.conn, query_dict['tbsp_Query'])
        dict_tbsp = ibm_db.fetch_assoc(tbsp_stmt)
        tbsp=[[] for i in range(0,self.NODENUM)]
        while dict_tbsp != False:
            tbspname=dict_tbsp["TBSP_NAME"]
            node_num=dict_tbsp["DBPARTITIONNUM"]
            self.listForoutput[node_num].append("db2.tbsp_type[" + tbspname + "]|" + str(dict_tbsp["TBSP_TYPE"]) + '|')
            self.listForoutput[node_num].append("db2.tbsp_status[" + tbspname + "]|" + str(dict_tbsp["TBSP_STATUS"]) + '|')
            self.listForoutput[node_num].append("db2.tbsp_state[" + tbspname + "]|" + str(dict_tbsp["TBSP_STATE"]) + '|')
            self.listForoutput[node_num].append("db2.tbsp_pagesize[" + tbspname + "]|" + str(dict_tbsp["TBSP_PAGE_SIZE"]) + '|')
            self.listForoutput[node_num].append("db2.tbsp_autoresize[" + tbspname + "]|" + str(dict_tbsp["TBSP_AUTO_RESIZE_ENABLED"]) + '|')
            self.listForoutput[node_num].append("db2.tbsp_used[" + tbspname + "]|" + str(dict_tbsp["USED_KB"]) + '|')
            self.listForoutput[node_num].append("db2.tbsp_total[" + tbspname + "]|" + str(dict_tbsp["TBSP_TOTAL_SIZE_KB"]) + '|')
            self.listForoutput[node_num].append("db2.tbsp_percent[" + tbspname + "]|" + str(dict_tbsp["TBSP_UTILIZATION_PERCENT"]) + '%' + '|')
            self.listForoutput[node_num].append("db2.tbsp_extent[" + tbspname + "]|" + str(dict_tbsp["TBSP_EXTENT_SIZE"]) + '|')
            self.listForoutput[node_num].append("db2.tbsp_hwm[" + tbspname + "]|" + str(dict_tbsp["TBSP_PAGE_TOP"]) + '|')
            self.listForoutput[node_num].append("db2.tbsp_judge[" + tbspname + "]|" + str(dict_tbsp["TBSP_UTILIZATION_JUDGE"]) + '|')
            tbsp[node_num] += [{'{#TBSPNAME}':tbspname}]
            dict_tbsp = ibm_db.fetch_assoc(tbsp_stmt)
        for node_num in range(0,self.NODENUM):
            self.discovery_dump(tbsp,node_num,"db2_tbsp_discovery|")

        log_stmt = ibm_db.exec_immediate(self.conn,query_dict['Log_Query'])
        dict_log = ibm_db.fetch_assoc(log_stmt)
        while dict_log !=False:
            node_num=dict_log["DBPARTITIONNUM"]
            tranlog_used = dict_log["TRANLOG_USED"]
            tranlog_total = dict_log["TRANLOG_TOTAL"]
            tranlog_percent = dict_log["LOG_USE_PERCENT"]
            dict_log = ibm_db.fetch_assoc(log_stmt)
            self.listForoutput[node_num].append("db2.log_used|" + str(tranlog_used) + '|')
            self.listForoutput[node_num].append("db2.log_total|" + str(tranlog_total) + '|')
            self.listForoutput[node_num].append("db2.log_percent|" + str(tranlog_percent) + '|')



    def collect_DB_config_globalVar_info(self):
        try:
            REG_stmt=ibm_db.exec_immediate(self.conn,query_dict['REG_Query'])
            REG_dict=ibm_db.fetch_assoc(REG_stmt)
            while REG_dict !=False:
                node_num=REG_dict["DBPARTITIONNUM"]
                self.listForoutput[node_num].append("db2set." + REG_dict["REG_VAR_NAME"] + "|" + REG_dict["REG_VAR_VALUE"] + '|')
                REG_dict=ibm_db.fetch_assoc(REG_stmt)

            DBMCFG_stmt=ibm_db.exec_immediate(self.conn,query_dict['DBMCFG_Query'])
            dbmcfg_dict=ibm_db.fetch_assoc(DBMCFG_stmt)
            while dbmcfg_dict is not False:
                dbmcfgname = str(dbmcfg_dict["NAME"])
                dbmvalue = str(dbmcfg_dict["VALUE"])
                self.listForoutput[0].append("dbmcfg." + dbmcfgname + "|" + dbmvalue + '|')
                dbmcfg_dict=ibm_db.fetch_assoc(DBMCFG_stmt)


            DBCFG_stmt = ibm_db.exec_immediate(self.conn, query_dict['DBCFG_Query'])
            dbcfg_dict = ibm_db.fetch_assoc(DBCFG_stmt)
            while dbcfg_dict != False:
                node_num=dbcfg_dict["DBPARTITIONNUM"]
                dbcfgname=dbcfg_dict["NAME"]
                self.listForoutput[node_num].append("db2cfg." + dbcfgname + "|" + dbcfg_dict["VALUE"] + '|')
                dbcfg_dict = ibm_db.fetch_assoc(DBCFG_stmt)
        except Exception,e:
            print e

    def collect_DB_perf_info(self):
        try:
            memory=[[] for i in range(0,self.NODENUM)]
            mem_stmt = ibm_db.exec_immediate(self.conn, query_dict['Memory_Query'])
            mem_dict = ibm_db.fetch_assoc(mem_stmt)
            while mem_dict != False:
                node_num=mem_dict["DBPARTITIONNUM"]
                self.listForoutput[node_num].append("db2.memvale[" + str(mem_dict["BP_NAME"]) + "]|" + str(mem_dict["POOL_CUR_SIZE"]) + 'MB' + '|')
                self.listForoutput[node_num].append("db2.mem_hitratio[" + str(mem_dict["BP_NAME"]) + "]|" + str(mem_dict["BP_HITRATIO"]) + '|')
                memname = mem_dict["BP_NAME"]
                memory[node_num] += [{'{#MEMPOOL}':memname}]
                mem_dict = ibm_db.fetch_assoc(mem_stmt)
            for node_num in range(0,self.NODENUM):
                self.discovery_dump(memory,node_num,"db2_mempool_discovery|")

            if instname == self.connDB.username :
                topsql=[[] for i in range(0,self.NODENUM)]
                topsql_stmt = ibm_db.exec_immediate(self.conn, query_dict['TopSQL_Query'])
                topsql_dict = ibm_db.fetch_assoc(topsql_stmt)
                while topsql_dict != False:
                    node_num=topsql_dict["DBPARTITIONNUM"]
                    sqlid = str(topsql_dict["NUMBER"])
                    self.listForoutput[node_num].append("db2.appl_handle[" + sqlid + "]|" + str(topsql_dict["APPLICATION_HANDLE"]) + '|')
                    self.listForoutput[node_num].append("db2.appl_status[" + sqlid + "]|" + str(topsql_dict["ACTIVITY_STATE"]) + '|')
                    self.listForoutput[node_num].append("db2.appl_name[" + sqlid + "]|" + str(topsql_dict["APPLICATION_NAME"]) + '|')
                    self.listForoutput[node_num].append("db2.num_execution[" + sqlid + "]|" + str(topsql_dict["NUM_EXECUTIONS"]) + '|')
                    self.listForoutput[node_num].append("db2.runtime[" + sqlid + "]|" + str(topsql_dict["TOTAL_EXEC_TIME"]) + '|')
                    self.listForoutput[node_num].append("db2.waittime[" + sqlid + "]|" + str(topsql_dict["TOTAL_CPU_TIME"]) + '|')
                    self.listForoutput[node_num].append("db2.sql_readrows[" + sqlid + "]|" + str(topsql_dict["ROWS_READ"]) + '|')
                    tempstr=re.sub(' +',' ',str(topsql_dict["STMT_TEXT"]))
                    self.listForoutput[node_num].append("db2.sql_text[" + sqlid + "]|" + tempstr+ '|')
                    topsql[node_num] += [{'{#SQLID}':sqlid}]
                    topsql_dict = ibm_db.fetch_assoc(topsql_stmt)
                for node_num in range(0,self.NODENUM):
                    self.discovery_dump(topsql,node_num,"db2_sql_discovery|")

            select_dbdiff_stmt = ibm_db.exec_immediate(self.conn, query_dict['dbsnap_Query'])
            perf_dict = ibm_db.fetch_assoc(select_dbdiff_stmt)
            while perf_dict != False:
                node_num=perf_dict["DBPARTITIONNUM"]
                self.listForoutput[node_num].append("db2.start_montime|" + str(perf_dict["START_TIMESTAMP"]) + '|')
                self.listForoutput[node_num].append("db2.end_montime|" + str(perf_dict["END_TIMESTAMP"]) + '|')
                self.listForoutput[node_num].append("db2.diff_time|" + str(perf_dict["REAL_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.elapsed_exec_time_s|" + str(perf_dict["ELAPSED_EXEC_TIME_S"]) + '|')
                self.listForoutput[node_num].append("db2.avg_trans_time|" + str(perf_dict["AVG_TRANS_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.avg_sql_time|" + str(perf_dict["AVG_SQL_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.avgtpm|" + str(perf_dict["TPM"]) + '|')
                self.listForoutput[node_num].append("db2.query_per_trans|" + str(perf_dict["QUERY_PER_TRANS"]) + '|')
                self.listForoutput[node_num].append("db2.success_trans_ratio|" + str(perf_dict["SUCCESS_TRANS_RATIO"]) + '%' + '|')
                self.listForoutput[node_num].append("db2.read_vs_write|" + str(perf_dict["READ_VS_WRITE"]) + ':1' + '|')
                self.listForoutput[node_num].append("db2.agent_cur|" + str(perf_dict["CUR_AGENT"]) + '|')
                self.listForoutput[node_num].append("db2.conn_count|" + str(perf_dict["AVG_CONS"]) + '|')
                self.listForoutput[node_num].append("db2.conn_current|" + str(perf_dict["APPLS_CUR_CONS"]) + '|')
                self.listForoutput[node_num].append("db2.sort_overflows_percent|" + str(perf_dict["SORT_OVERFLOWS_PERCENT"]) + '|')
                self.listForoutput[node_num].append("db2.avg_sort_time|" + str(perf_dict["AVG_SORT_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.sort_percent|" + str(perf_dict["SORT_PERCENT"]) + '|')
                self.listForoutput[node_num].append("db2.lock_timeouts|" + str(perf_dict["LOCK_TIMEOUTS"]) + '|')
                self.listForoutput[node_num].append("db2.lock_waits|" + str(perf_dict["LOCK_WAITS"]) + '|')
                self.listForoutput[node_num].append("db2.lock_escals|" + str(perf_dict["LOCK_ESCALS"]) + '|')
                self.listForoutput[node_num].append("db2.deadlocks|" + str(perf_dict["DEADLOCKS"]) + '|')
                self.listForoutput[node_num].append("db2.avg_lock_wait_time|" + str(perf_dict["AVG_LOCK_WAIT_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.selectperread|" + str(perf_dict["SELECTPERREAD"]) + '|')
                self.listForoutput[node_num].append("db2.data_sync_read_percent|" + str(perf_dict["DATA_SYNC_READ_PERCENT"]) + '%' + '|')
                self.listForoutput[node_num].append("db2.data_sync_write_percent|" + str(perf_dict["DATA_SYNC_WRITE_PERCENT"]) + '%' + '|')
                self.listForoutput[node_num].append("db2.modified_rows_per_trans|" + str(perf_dict["MODIFIED_ROWS_PER_TRANS"]) + '|')
                self.listForoutput[node_num].append("db2.rows_read_per_sql|" + str(perf_dict["ROWS_READ_PER_SQL"]) + '|')
                self.listForoutput[node_num].append("db2.total_sort_time_s|" + str(perf_dict["TOTAL_SORT_TIME_S"]) + '|')
                self.listForoutput[node_num].append("db2.total_lock_wait_time_s|" + str(perf_dict["TOTAL_LOCK_WAIT_TIME_S"]) + '|')
                self.listForoutput[node_num].append("db2.pool_read_time_s|" + str(perf_dict["POOL_READ_TIME_S"]) + '|')
                self.listForoutput[node_num].append("db2.pool_async_read_time_s|" + str(perf_dict["POOL_ASYNC_READ_TIME_S"]) + '|')
                self.listForoutput[node_num].append("db2.pool_write_time_s|" + str(perf_dict["POOL_WRITE_TIME_S"]) + '|')
                self.listForoutput[node_num].append("db2.pool_async_write_time_s|" + str(perf_dict["POOL_ASYNC_WRITE_TIME_S"]) + '|')
                self.listForoutput[node_num].append("db2.direct_read_time_s|" + str(perf_dict["DIRECT_READ_TIME_S"]) + '|')
                self.listForoutput[node_num].append("db2.direct_write_time_s|" + str(perf_dict["DIRECT_WRITE_TIME_S"]) + '|')
                self.listForoutput[node_num].append("db2.total_prefetch_wait_time_s|" + str(perf_dict["TOTAL_PREFETCH_WAIT_TIME_S"]) + '|')
                self.listForoutput[node_num].append("db2.log_read_time_s|" + str(perf_dict["LOG_READ_TIME_S"]) + '|')
                self.listForoutput[node_num].append("db2.log_write_time_s|" + str(perf_dict["LOG_WRITE_TIME_S"]) + '|')
                self.listForoutput[node_num].append("db2.unread_prefetch_percent|" + str(perf_dict["UNREAD_PREFETCH_PERCENT"]) + '%' + '|')
                self.listForoutput[node_num].append("db2.prefetch_wait_time_percent|" + str(perf_dict["PREFETCH_WAIT_TIME_PERCENT"]) + '%' + '|')
                self.listForoutput[node_num].append("db2.log_part_ratio|" + str(perf_dict["LOG_PART_RATIO"]) + '%' + '|')
                self.listForoutput[node_num].append("db2.avg_async_data_page|" + str(perf_dict["AVG_ASYNC_DATA_PAGE"]) + '|')
                self.listForoutput[node_num].append("db2.avg_async_index_page|" + str(perf_dict["AVG_ASYNC_INDEX_PAGE"]) + '|')
                self.listForoutput[node_num].append("db2.avg_sync_read_time|" + str(perf_dict["AVG_SYNC_READ_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.avg_async_read_time|" + str(perf_dict["AVG_ASYNC_READ_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.sync_pool_write_time|" + str(perf_dict["SYNC_POOL_WRITE_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.async_pool_write_time|" + str(perf_dict["ASYNC_POOL_WRITE_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.avg_direct_read_time|" + str(perf_dict["AVG_DIRECT_READ_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.avg_direct_write_time|" + str(perf_dict["AVG_DIRECT_WRITE_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.avg_log_read_time|" + str(perf_dict["AVG_LOG_READ_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.avg_log_write_time|" + str(perf_dict["AVG_LOG_WRITE_TIME"]) + '|')
                self.listForoutput[node_num].append("db2.commit_sql_stmts|" + str(perf_dict["DB_COMMIT_SQL_STMTS"])+'|')
                self.listForoutput[node_num].append("db2.conn_hwm|" + str(perf_dict["DB_CONN_HWM"])+'|')
                self.listForoutput[node_num].append("db2.priv_memory_hitratio|" + str(perf_dict["PRIV_MEMORY_HITRATIO"]) + '|')
                self.listForoutput[node_num].append("db2.shr_memory_hitratio|" + str(perf_dict["SHR_MEMORY_HITRATIO"]) + '|')
                perf_dict = ibm_db.fetch_assoc(select_dbdiff_stmt)

            Agent_stmt = ibm_db.exec_immediate(self.conn, query_dict['Agent_Query'])
            agent_dict = ibm_db.fetch_assoc(Agent_stmt)
            while agent_dict != False:
                node_num=agent_dict["DBPARTITIONNUM"]
                self.listForoutput[node_num].append("db2.agent_max|" + str(agent_dict["MAX_AGENT"]) + '|')
                self.listForoutput[node_num].append("db2.appl_max|" + str(agent_dict["MAX_APPL"]) + '|')
                agent_dict = ibm_db.fetch_assoc(Agent_stmt)

            ibm_db.exec_immediate(self.conn, "DELETE FROM DB_BASELINE")
            ibm_db.exec_immediate(self.conn, "INSERT INTO DB_BASELINE select *  from sysibmadm.snapdb")
        except Exception,e:
            print e

    def prefind_underlying_issues(self):
        try:
            Stats_stmt=ibm_db.exec_immediate(self.conn, query_dict['Stats_Query'])
            stats_dict=ibm_db.fetch_assoc(Stats_stmt)
            while stats_dict !=False:
                tbspname = stats_dict["TABLESPACE"]
                self.listForoutput[0].append("db2.tabnumber[" + tbspname + "]|" + str(stats_dict["NO_OF_OBJ"]) + '|')
                self.listForoutput[0].append("db2.laststattime[" + tbspname + "]|" + str(stats_dict["LATEST_STATS_TIME"]) + '|')
                self.listForoutput[0].append("db2.oldeststattime[" + tbspname + "]|" + str(stats_dict["OLDEST_STATS_TIME"]) + '|')
                self.listForoutput[0].append("db2.largestnpage[" + tbspname + "]|" + str(stats_dict["LARGEST_NPAGES"]) + '|')
                self.listForoutput[0].append("db2.largestfpage[" + tbspname + "]|" + str(stats_dict["LARGEST_FPAGES"]) + '|')
                self.listForoutput[0].append("db2.largestcard[" + tbspname + "]|" + str(stats_dict["LARGEST_CARD"]) + '|')
                stats_dict= ibm_db.fetch_assoc(Stats_stmt)

            unavailableObj=[[] for i in range(0,self.NODENUM)]
            unavailable_Item_stmt=ibm_db.exec_immediate(self.conn, query_dict['unavailable_Item'])
            unavailable_Item_dict=ibm_db.fetch_assoc(unavailable_Item_stmt)
            while unavailable_Item_dict !=False:
            #   #print unavailable_Item_dict["TRIGSCHEMA"]
                TRIGNAME=unavailable_Item_dict["TRIGNAME"]
                self.listForoutput[0].append("db2.TRIGSCHEMA["+TRIGNAME+"]|"+str(unavailable_Item_dict["TRIGSCHEMA"])+'|')
            #   self.listForoutput[0].append("db2.TRIGNAME["+TRIGNAME+"]|"+str(unavailable_Item_dict["TRIGNAME"])+'|')
                self.listForoutput[0].append("db2.TYPE["+TRIGNAME+"]|"+str(unavailable_Item_dict["TYPE"])+'|')
                self.listForoutput[0].append("db2.valid["+TRIGNAME+"]|"+str(unavailable_Item_dict["VALID"])+'|')
                self.listForoutput[0].append("db2.CREATE_TIME["+TRIGNAME+"]|"+str(unavailable_Item_dict["CREATE_TIME"])+'|')
                self.listForoutput[0].append("db2.ALTER_TIME["+TRIGNAME+"]|"+str(unavailable_Item_dict["ALTER_TIME"])+'|')
                unavailableObj[0]+=[{'{#UnavailalbeObjName}':TRIGNAME}]
                unavailable_Item_dict=ibm_db.fetch_assoc(unavailable_Item_stmt)
            self.discovery_dump(unavailableObj,0,"db2_unavailableObj_discovery|")

            table=[[] for i in range(0,self.NODENUM)]
            table_stmt = ibm_db.exec_immediate(self.conn, query_dict['Table_Query'])
            dict_table = ibm_db.fetch_assoc(table_stmt)
            while dict_table is not  False:
                tabname = dict_table["TABLE"]
                node_num=dict_table["DBPARTITIONNUM"]
                self.listForoutput[node_num].append("db2.tab_available[" + tabname + "]|" + str(dict_table["AVAILABLE"]) + '|')
                self.listForoutput[node_num].append("db2.tab_loadstatus[" + tabname + "]|" + str(dict_table["LOAD_STATUS"]) + '|')
                self.listForoutput[node_num].append("db2.tab_scans[" + tabname + "]|" + str(dict_table["TABLE_SCANS"]) + '|')
                self.listForoutput[node_num].append("db2.tab_rowsread[" + tabname + "]|" + str(dict_table["ROWS_READ"]) + '|')
                self.listForoutput[node_num].append("db2.tab_overflow[" + tabname + "]|" + str(dict_table["OVERFLOW_ACCESSES"]) + '|')
                self.listForoutput[node_num].append("db2.tab_pagereorg[" + tabname + "]|" + str(dict_table["PAGE_REORGS"]) + '|')
                self.listForoutput[node_num].append("db2.tab_usage[" + tabname + "]|" + str(dict_table["TABLE_USAGE"]) + '%' + '|')
                table[node_num] += [{'{#TABNAME}':tabname}]
                dict_table = ibm_db.fetch_assoc(table_stmt)
            for node_num in range(0,self.NODENUM):
                self.discovery_dump(table,node_num,"db2_table_discovery|")

            DB2_PRODUCT_VERSION_SET=set(['v9.7','v9.8','v10','v11'])
            sub_version=version.split()[1].split(".")
            if sub_version[0] in DB2_PRODUCT_VERSION_SET or (sub_version[0]+"."+sub_version[1]) in DB2_PRODUCT_VERSION_SET:
                index=[[] for i in range(0,self.NODENUM)]
                index_stmt = ibm_db.exec_immediate(self.conn, query_dict['Index_Query'])
                dict_index = ibm_db.fetch_assoc(index_stmt)
                while dict_index !=False:
                    idxname = dict_index["INDNAME"]
                    self.listForoutput[0].append("db2.idx_scans[" + idxname + "]|" + str(dict_index["INDEX_SCANS"]) + '|')
                    self.listForoutput[0].append("db2.idx_splits[" + idxname + "]|" + str(dict_index["SPLITS"]) + '|')
                    self.listForoutput[0].append("db2.idx_pseudo_emptypage[" + idxname + "]|" + str(dict_index["PSEUDO_EMPTY_PAGES"]) + '|')
                    self.listForoutput[0].append("db2.idx_pseudo_del[" + idxname + "]|" + str(dict_index["PSEUDO_DELETES"]) + '|')
                    index[0] += [{'{#IDXNAME}':idxname}]
                    dict_index = ibm_db.fetch_assoc(index_stmt)
                self.discovery_dump(index,0,"db2_index_discovery|")

            diagnum=[[] for i in range(0,self.NODENUM)]
            diag_count=0
            diag_stmt = ibm_db.exec_immediate(self.conn, query_dict['diag_Query'])
            dict_diag = ibm_db.fetch_assoc(diag_stmt)
            while dict_diag != False:
                diagid = str(dict_diag["ROW_NUMBER"])
                self.listForoutput[0].append("db2.diagstart[" + diagid + "]|" + str(dict_diag["START_TIME"]) + '|')
                self.listForoutput[0].append("db2.diagend[" + diagid + "]|" + str(dict_diag["END_TIME"]) + '|')
                self.listForoutput[0].append("db2.diagcount[" + diagid + "]|" + str(dict_diag["COUNT"]) + '|')
                self.listForoutput[0].append("db2.diaglevel[" + diagid + "]|" + dict_diag["LEVEL"] + '|')
                self.listForoutput[0].append("db2.diagdbname[" + diagid + "]|" + str(dict_diag["DBNAME"]) + '|')
                self.listForoutput[0].append("db2.diagauth[" + diagid + "]|" + dict_diag["AUTH_ID"] + '|')
                self.listForoutput[0].append("db2.diagfunc[" + diagid + "]|" + str(dict_diag["FUNCTION"]) + '|')
                self.listForoutput[0].append("db2.diagmsg[" + diagid + "]|" + dict_diag["MSG"] + '|')
                diagnum[0] += [{'{#DIAGID}':diagid}]
                diag_count = diag_count + 1
                dict_diag = ibm_db.fetch_assoc(diag_stmt)
            self.listForoutput[0].append("db2.diagcount|" + str(diag_count) + '|')
            self.discovery_dump(diagnum,0,"db2_diag_discovery|")
            ibm_db.close(self.conn)
        except Exception,e:
            print e

    def discovery_dump(self,container,nodeNum,description):
        item_discovery=json.dumps(
            {'data':container[nodeNum]},
             sort_keys=True,
            separators=(',',':'))
        deco=description+item_discovery+'|'
        self.discoList[nodeNum].append(deco)

def parse_hostfile(inputfile,hostList):
    fin=open(inputfile,'rt')
    isScan=True
    lineno=0
    while isScan:
        try:
            line=fin.readline().replace('\r','')
            if not line:
                break
            else:
                lineno+=1
            if line.startswith('#') is not True:
                host_detail=line.split()
                if len(host_detail)==5:
                    if re.match('\w+',host_detail[0]) and re.match('\w+',host_detail[1]) and (re.match('\w+',host_detail[3]) or re.match('\d+\.\d+.\d+.\d+',host_detail[3])) and re.match('\d+',host_detail[4]):
                        hostList.append((host_detail[0],host_detail[1],host_detail[2],host_detail[3],host_detail[4]))
                    else:
                        print "the formatters the line %d is not correct,check it please!" %lineno
                        break
                elif len(host_detail)==6:
                    if re.match('\w+',host_detail[0]) and re.match('\w+',host_detail[1]) and (re.match('\w+',host_detail[3]) or re.match('\d+\.\d+.\d+.\d+',host_detail[3])) and re.match('\d+',host_detail[4]) and re.match('\w+',host_detail[5]):
                        hostList.append((host_detail[0],host_detail[1],host_detail[2],host_detail[3],host_detail[4],host_detail[5]))
                    else:
                        print "the formatters the line %d is not correct,check it please!" %lineno
                        break
                else:
                    print "the host at the line %d is short of some items.check it please!" %lineno
                    break
        except StopIteration as err:
            isScan=False
        except Exception as other:
            print('errors occured in the function:',other)
            sys.exit(1)
    fin.close()

def checkArgument():
   parser = OptionParser(usage=" %prog [-d database] [-U username] [-P password] [-I <interface or address>] [-p port] [-a authentication] \n\t%prog [-i <input file>]",version="%prog 3.0")

   parser.add_option("-d", "--database", action="store",
              type="string",
              dest="db",
              help="specified DB2 database name")
   parser.add_option("-U", "--user", action="store",
              type="string",
              dest="user",
              help="specified connect User need SYSMON authorize")
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
   parser.add_option("-a", "--authentication", action="store",
              type="string",
              dest="authentication",
              help="sperified the authentication function for check database")
   parser.add_option("-i", "--input", action="store",
              type="string",
              dest="filename",
              help="sperified the file include connection infomation")
   (options, args) = parser.parse_args()

   hostList=[]
   if options.db and options.user and options.password and options.address and options.port and options.filename is None:
       hosts=(options.db,options.user,options.password,options.address,options.port)
       hostList.append(hosts)
   elif options.db and options.user and options.password and options.address and options.port and options.authentication and options.filename is None:
       hosts=(options.db,options.user,options.password,options.address,options.port,options.authentication)
       hostList.append(hosts)
   elif options.db is None and options.user is None and options.password is None and options.address is None and options.port is None and options.filename:
       parse_hostfile(options.filename,hostList)
   else:
       parser.print_help()
   return hostList
def worker_checkdb2(*host):
    if len(host)==5:
        db,user,password,address,port=host
        print "starting to check DB2:%s at port %s of %s ..." %(db,port,address)
        conn=connDB(db,user,password,address,port)
    elif len(host)==6:
        db,user,password,address,port,authentication=host
        print "starting to check DB2:%s at port %s of %s ..." %(db,port,address)
        conn=connDB(db,user,password,address,port,authentication)

    ckdb=checkDB(conn)
    ckdb.collect_db_info()
    ckdb.collect_DB_config_globalVar_info()
    ckdb.collect_capacity_info()
    ckdb.collect_DB_perf_info()
    ckdb.prefind_underlying_issues()
    ckdb.write_result_to_file()
    print "ending to check DB2:%s at port %s of %s ..." %(db,port,address)
def main():
    hostList=checkArgument()
    NUMBER_OF_PROCESSES=len(hostList)
    for i in range(len(hostList)):
        pro=Process(target=worker_checkdb2,args=(hostList[i]))
        pro.start()

    for i in range(len(hostList)):
        pro.join()

if __name__=="__main__":
    main()
