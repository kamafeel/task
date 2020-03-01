MERGE INTO PROC_SCHEDULE_RUN_INDEX a USING (SELECT 				  to_char(sysdate,'yyyy-MM') as MONTH_ID,               team_code,               xmlid,               proc_name,               RUN_FREQ,               2 as index_type,               round(sum((to_date(l.exec_time, 'yyyy-mm-dd hh24:mi:ss') -                          to_date(l.end_time, 'yyyy-mm-dd hh24:mi:ss')) * 24 * 60) /                      count(1),                      0) as index_value,               0 as index_offset,               to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as update_time           FROM proc_schedule_log l          where LENGTH(exec_time) > 0            AND LENGTH(end_time) > 0            AND to_date(l.end_time, 'yyyy-mm-dd hh24:mi:ss') >                to_date(l.exec_time, 'yyyy-mm-dd hh24:mi:ss')            and to_date(substr(l.start_time, 1, 10), 'yyyy-mm-dd') >=                to_date(to_char(sysdate - 30, 'yyyy-mm-dd'), 'yyyy-mm-dd')            and valid_flag = '0'  and task_state=6          group by xmlid,proc_name,team_code,RUN_FREQ) b ON (a.xmlid = b.xmlid and a.index_type = b.index_type and a.MONTH_ID = b.MONTH_ID) WHEN MATCHED THEN   UPDATE SET a.index_value = b.index_value, a.update_time = b.update_time WHEN NOT MATCHED THEN   INSERT     (MONTH_ID,team_code,xmlid,proc_name, RUN_FREQ, index_type, index_value, index_offset, update_time)   VALUES     (      b.MONTH_ID,      b.team_code,      b.xmlid,      b.proc_name,      b.RUN_FREQ,      b.index_type,      b.index_value,      b.index_offset,      b.update_time)
MERGE INTO PROC_SCHEDULE_RUN_INDEX a USING (SELECT                to_char(sysdate,'yyyy-MM') as MONTH_ID,               team_code,               xmlid,               proc_name,               'day' as RUN_FREQ,               0 as index_type,               TO_CHAR(round(sum(TO_NUMBER(TO_DATE(exec_time,                                                   'YYYY-MM-DD HH24:MI:SS') -                                           TO_DATE('1970-01-01 8:0:0',                                                   'YYYY-MM-DD HH24:MI:SS')) * 24 * 60 * 60 * 1000) /                             count(1),                             0) / (1000 * 60 * 60 * 24) +                       TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH:MI:SS'),                       'HH24:MI:SS') index_value,                            0 as index_offset,               to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as update_time          FROM proc_schedule_log         where exec_time is not null           and RUN_FREQ = 'day'           and valid_flag = '0'           and to_date(substr(exec_time, 1, 10), 'yyyy-mm-dd') >=               to_date(to_char(sysdate - 30, 'yyyy-mm-dd'), 'yyyy-mm-dd')         group by xmlid,proc_name,team_code) b ON (a.xmlid = b.xmlid and a.index_type = b.index_type and a.MONTH_ID = b.MONTH_ID) WHEN MATCHED THEN   UPDATE SET a.index_value = b.index_value, a.update_time = b.update_time WHEN NOT MATCHED THEN   INSERT     (MONTH_ID,team_code,xmlid,proc_name, RUN_FREQ, index_type, index_value, index_offset, update_time)   VALUES     (      b.MONTH_ID,      b.team_code,      b.xmlid,      b.proc_name,      b.RUN_FREQ,      b.index_type,      b.index_value,      b.index_offset,      b.update_time)
MERGE INTO PROC_SCHEDULE_RUN_INDEX a USING (SELECT                to_char(sysdate,'yyyy-MM') as MONTH_ID,               team_code,               xmlid,               proc_name,               'day' as RUN_FREQ,               1 as index_type,               TO_CHAR(round(sum(TO_NUMBER(TO_DATE(end_time,                                                   'YYYY-MM-DD HH24:MI:SS') -                                           TO_DATE('1970-01-01 8:0:0',                                                   'YYYY-MM-DD HH24:MI:SS')) * 24 * 60 * 60 * 1000) /                             count(1),                             0) / (1000 * 60 * 60 * 24) +                       TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH:MI:SS'),                       'HH24:MI:SS') index_value,                            0 as index_offset,               to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as update_time          FROM proc_schedule_log         where end_time is not null           and RUN_FREQ = 'day'           and valid_flag = '0'           and task_state=6           and to_date(substr(end_time, 1, 10), 'yyyy-mm-dd') >=               to_date(to_char(sysdate - 30, 'yyyy-mm-dd'), 'yyyy-mm-dd')         group by xmlid,proc_name,team_code) b ON (a.xmlid = b.xmlid and a.index_type = b.index_type and a.MONTH_ID = b.MONTH_ID) WHEN MATCHED THEN   UPDATE SET a.index_value = b.index_value, a.update_time = b.update_time WHEN NOT MATCHED THEN   INSERT     (MONTH_ID,team_code,xmlid,proc_name, RUN_FREQ, index_type, index_value, index_offset, update_time)   VALUES     (      b.MONTH_ID,      b.team_code,      b.xmlid,      b.proc_name,      b.RUN_FREQ,      b.index_type,      b.index_value,      b.index_offset,      b.update_time)
MERGE INTO PROC_SCHEDULE_RUN_INDEX a USING (SELECT                to_char(sysdate,'yyyy-MM') as MONTH_ID,               team_code,               xmlid,               proc_name,               'month' as RUN_FREQ,               0 as index_type,               TO_CHAR(round(sum(TO_NUMBER(TO_DATE(exec_time,                                                   'YYYY-MM-DD HH24:MI:SS') -                                           TO_DATE('1970-01-01 8:0:0',                                                   'YYYY-MM-DD HH24:MI:SS')) * 24 * 60 * 60 * 1000) /                             count(1),                             0) / (1000 * 60 * 60 * 24) +                       TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH:MI:SS'),                       'YYYY-MM-DD HH24:MI:SS') index_value,                            0 as index_offset,               to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as update_time          FROM proc_schedule_log         where exec_time is not null           and RUN_FREQ = 'month'           and valid_flag = '0'           and to_date(substr(exec_time, 1, 10), 'yyyy-mm-dd') >=               to_date(to_char(sysdate - 365, 'yyyy-mm-dd'), 'yyyy-mm-dd')         group by xmlid,proc_name,team_code) b ON (a.xmlid = b.xmlid and a.index_type = b.index_type and a.MONTH_ID = b.MONTH_ID) WHEN MATCHED THEN   UPDATE SET a.index_value = b.index_value, a.update_time = b.update_time WHEN NOT MATCHED THEN   INSERT     (MONTH_ID,team_code,xmlid,proc_name, RUN_FREQ, index_type, index_value, index_offset, update_time)   VALUES     (      b.MONTH_ID,      b.team_code,      b.xmlid,      b.proc_name,      b.RUN_FREQ,      b.index_type,      b.index_value,      b.index_offset,      b.update_time)
MERGE INTO PROC_SCHEDULE_RUN_INDEX a USING (SELECT                to_char(sysdate,'yyyy-MM') as MONTH_ID,               team_code,               xmlid,               proc_name,               'month' as RUN_FREQ,               1 as index_type,               TO_CHAR(round(sum(TO_NUMBER(TO_DATE(end_time,                                                   'YYYY-MM-DD HH24:MI:SS') -                                           TO_DATE('1970-01-01 8:0:0',                                                   'YYYY-MM-DD HH24:MI:SS')) * 24 * 60 * 60 * 1000) /                             count(1),                             0) / (1000 * 60 * 60 * 24) +                       TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH:MI:SS'),                       'YYYY-MM-DD HH24:MI:SS') index_value,                            0 as index_offset,               to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as update_time          FROM proc_schedule_log         where end_time is not null           and RUN_FREQ = 'month'           and valid_flag = '0'   and task_state=6         and to_date(substr(end_time, 1, 10), 'yyyy-mm-dd') >=               to_date(to_char(sysdate - 365, 'yyyy-mm-dd'), 'yyyy-mm-dd')         group by xmlid,proc_name,team_code) b ON (a.xmlid = b.xmlid and a.index_type = b.index_type and a.MONTH_ID = b.MONTH_ID) WHEN MATCHED THEN   UPDATE SET a.index_value = b.index_value, a.update_time = b.update_time WHEN NOT MATCHED THEN   INSERT     (MONTH_ID,team_code,xmlid,proc_name, RUN_FREQ, index_type, index_value, index_offset, update_time)   VALUES     (      b.MONTH_ID,      b.team_code,      b.xmlid,      b.proc_name,      b.RUN_FREQ,      b.index_type,      b.index_value,      b.index_offset,      b.update_time)