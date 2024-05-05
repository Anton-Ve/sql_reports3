COLUMN TASK_SEQ NEW_V TASK_SEQ_CURRENT
select (&TASK_SEQ_CURRENT.+1) TASK_SEQ from dual;

PROMPT VZ_TASK_BASE_LINE.&TASK_CAT..&TASK_CD..&TASK_SEQ_CURRENT..merge - start
declare
   l_seq        number := &TASK_SEQ_CURRENT;
   l_task_cd    VARCHAR2(16) := '&TASK_CD.';
   l_cat_cd     VARCHAR2(16) := '&TASK_CAT.';
   l_ENTITY_NM  VARCHAR2(32) := 's4';
   l_SHEET_NM   VARCHAR2(32) := '';
   l_SHEET_COL  VARCHAR2(32) := '';
   l_SQL_OP     VARCHAR2(7)  := 'CREATE';
   l_ENABLE     number := 1;
   l_EXPORT	    number := 0;
   l_LINE_BUILD number := 1;
   l_DESCR      clob := Q'##';
   l_SQL_HEADER clob := Q'##';
   l_SQL_QUERY  clob := Q'#select * from ${s4}#';
   l_SQL_DATA   clob := Q'#select 
   distinct
   tr.sp_id
  ,tr.item_id
  ,max(eq.install_dt) over (partition by eq.sp_id) install_dt
  ,it.badge_nbr
  ,max(dpp.adhoc_char_val) keep (dense_rank last order by dpp.effdt) over (partition by dpp.item_id) dpp
  ,max(ppv.char_val) keep (dense_rank last order by ppv.effdt) over (partition by ppv.item_id) ppv
from ${s3} tr
  join ci_sp_eq eq on eq.sp_id=tr.sp_id and eq.item_id_eq=tr.item_id
  join ci_item it on it.item_id=tr.item_id
  left join ci_item_char dpp on dpp.item_id=it.item_id and dpp.char_type_cd='DT-P-POV'
  left join ci_item_char ppv on ppv.item_id=it.item_id and ppv.char_type_cd='PER-POV'
where 1=1
and tr.npp=1
   #';
   l_index number;
begin
    SELECT t.idx into l_index from VZ_TASK_BASE t where t.TASK_CD = l_task_cd and t.CAT_CD = l_cat_cd;

	merge into VZ_TASK_BASE_LINE tgt
	using (
	   SELECT l_index      TASK_IDX
            , l_seq 	   SEQ
            , l_ENTITY_NM  ENTITY_NM
            , l_SHEET_NM   SHEET_NM
            , l_SHEET_COL  SHEET_COL
            , l_SQL_OP 	   SQL_OP
            , l_ENABLE 	   ENABLE
            , l_EXPORT 	   EXPORT
            , l_LINE_BUILD LINE_BUILD
            , l_DESCR 	   DESCR
            , l_SQL_HEADER SQL_HEADER
            , l_SQL_QUERY  SQL_QUERY
            , l_SQL_DATA   SQL_DATA
        FROM dual
	) src
	on (tgt.TASK_IDX = src.TASK_IDX and tgt.SEQ = src.SEQ)
	WHEN MATCHED THEN
		UPDATE set tgt.ENTITY_NM       = src.ENTITY_NM
				 , tgt.SHEET_NM        = src.SHEET_NM
                 , tgt.SHEET_COL       = src.SHEET_COL
				 , tgt.SQL_OP          = src.SQL_OP
				 , tgt.ENABLE          = src.ENABLE
				 , tgt.EXPORT          = src.EXPORT
				 , tgt.LINE_BUILD      = src.LINE_BUILD
				 , tgt.DESCR           = src.DESCR
                 , tgt.SQL_HEADER      = src.SQL_HEADER
				 , tgt.SQL_QUERY       = src.SQL_QUERY
				 , tgt.SQL_DATA        = src.SQL_DATA
        WHERE tgt.LINE_BUILD < src.LINE_BUILD
	WHEN NOT MATCHED THEN
		 INSERT( tgt.TASK_IDX, tgt.SEQ, tgt.ENTITY_NM, tgt.SHEET_NM, tgt.SHEET_COL, tgt.SQL_OP, tgt.ENABLE, tgt.EXPORT, tgt.LINE_BUILD, tgt.DESCR, tgt.SQL_HEADER, tgt.SQL_QUERY, tgt.SQL_DATA)  
		 VALUES( src.TASK_IDX, src.SEQ, src.ENTITY_NM, src.SHEET_NM, src.SHEET_COL, src.SQL_OP, src.ENABLE, src.EXPORT, src.LINE_BUILD, src.DESCR, src.SQL_HEADER, src.SQL_QUERY, src.SQL_DATA)  
	;
	commit;
end;
/
