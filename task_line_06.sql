COLUMN TASK_SEQ NEW_V TASK_SEQ_CURRENT
select (&TASK_SEQ_CURRENT.+1) TASK_SEQ from dual;

PROMPT VZ_TASK_BASE_LINE.&TASK_CAT..&TASK_CD..&TASK_SEQ_CURRENT..merge - start
declare
   l_seq        number := &TASK_SEQ_CURRENT;
   l_task_cd    VARCHAR2(16) := '&TASK_CD.';
   l_cat_cd     VARCHAR2(16) := '&TASK_CAT.';
   l_ENTITY_NM  VARCHAR2(32) := '';
   l_SHEET_NM   VARCHAR2(32) := '';
   l_SHEET_COL  VARCHAR2(32) := '';
   l_SQL_OP     VARCHAR2(7)  := 'PLSQL';
   l_ENABLE     number := 1;
   l_EXPORT	    number := 0;
   l_LINE_BUILD number := 1;
   l_DESCR      clob := Q'##';
   l_SQL_HEADER clob := Q'##';
   l_SQL_QUERY  clob := Q'##';
   l_SQL_DATA   clob := Q'#DECLARE
  cSn varchar2(16);
  dDt date;
  fKs varchar2(250);
  --
  cursor c0 is
    select distinct tt.sp_id from ${s1}_h tt;

BEGIN
  for r0 in c0 loop
    begin
      with temp as (
        select distinct
          cast(max(mh.mtr_config_id) keep (dense_rank last order by mh.removal_dttm) over (partition by mh.sp_id) as char(10)) mtr_config_id
          ,max(mh.removal_dttm) over (partition by mh.sp_id) removal_dttm
        from
          ci_sp_mtr_hist mh
        where 1=1
          and mh.removal_dttm is not null
          and mh.sp_id=r0.sp_id
      )
      select
        mt.serial_nbr
        ,t.removal_dttm
        ,l.descr
      into cSn,dDt,fKs
      from temp t
        join ci_mtr_config mc on mc.mtr_config_id=t.mtr_config_id
        join ci_mtr mt on mt.mtr_id=mc.mtr_id
        join ci_model_l l on mt.mfg_cd=l.mfg_cd and mt.model_cd=l.model_cd and l.language_cd='RUS'
      ;
    exception when NO_DATA_FOUND then cSn:=null; dDt:=null;fKs:=null; end;
    --
    update ${s1}_h t set t.serial_nbr_old=cSn,t.removal_dttm=dDt,t.model_old=fKs where t.sp_id=r0.sp_id;
    commit;
  end loop;
END;
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
