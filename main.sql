PROMPT TASK - init
DEFINE TASK_CAT   = 'LP'
DEFINE TASK_CD    = '04.101'
DEFINE TASK_SEQ_CURRENT = 0;
DEFINE TASK_BUILD = 6
PROMPT TASK.&TASK_CAT..&TASK_CD. - start
@@task.sql
@@task_link.sql
@@task_line_01.sql
@@task_line_02.sql
@@task_line_03.sql
@@task_line_04.sql
@@task_line_05.sql
@@task_line_06.sql
@@task_line_07.sql
@@task_line_08.sql
PROMPT TASK_SEQ_LAST=&TASK_SEQ_CURRENT
PROMPT TASK.&TASK_CAT..&TASK_CD. - finish
