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
  iNum number(1);
  --
  cursor c0 is select distinct tt.sp_id from ${s1}_h tt;
  cursor c1(s_id ci_sp.sp_id%TYPE) is 
    select 
      it.item_id
    from 
      ci_sp_eq eq
      join ci_item it on it.item_id=eq.item_id_eq and it.item_type_cd='TRI-I' and it.item_status_flg='A'
    where 1=1
      and eq.removal_dt is null
      and eq.sp_id=s_id
    order by 1
    ;
  cursor c2(s_id ci_sp.sp_id%TYPE) is 
    select distinct
      mh.sp_id
      ,mh.install_const
      ,mc.mtr_config_id
      ,mt.mtr_id
      ,mt.serial_nbr
      ,mt.mfg_cd
      ,mt.model_cd
      ,mt.mtr_type_cd
      ,mr.read_dttm
      ,rr1.reg_reading reg_reading1
      ,rr2.reg_reading reg_reading2
      ,rr3.reg_reading reg_reading3
      ,max(dpp.adhoc_char_val) keep (dense_rank last order by dpp.effdt) over (partition by dpp.mtr_id) dpp
      ,max(ppv.char_val) keep (dense_rank last order by ppv.effdt) over (partition by ppv.mtr_id) ppv
      ,(select z.descr
        from ci_char_val_l z
          where 1=1
            and z.char_type_cd='SMSYS-PU'
             and z.language_cd='RUS'
          and z.char_val=
      (select max(mc.char_val) keep(dense_rank last order by mc.effdt)
         from ci_mtr_char mc
          where 1=1
           and mc.char_type_cd='SMSYS-PU' 
             and mc.mtr_id=mt.mtr_id)) isu  
	from 
      ci_sp_mtr_hist mh
      join ci_sp_mtr_evt me on me.sp_mtr_hist_id=mh.sp_mtr_hist_id and me.sp_mtr_evt_flg='I'
           and me.seqno=(select max(seqno) from ci_sp_mtr_evt where sp_mtr_hist_id=me.sp_mtr_hist_id and sp_mtr_evt_flg=me.sp_mtr_evt_flg)
      join ci_mr mr on mr.mr_id=me.mr_id
      join ci_mtr_config mc on mc.mtr_config_id=mh.mtr_config_id
      join ci_mtr mt on mt.mtr_id=mc.mtr_id
        join ci_reg r1 on r1.mtr_id=mt.mtr_id and r1.read_seq=1
          join ci_reg_read rr1 on rr1.mr_id=mr.mr_id and rr1.reg_id=r1.reg_id
        left join ci_reg r2 on r2.mtr_id=mt.mtr_id and r2.read_seq=2
          left join ci_reg_read rr2 on rr2.mr_id=mr.mr_id and rr2.reg_id=r2.reg_id
        left join ci_reg r3 on r3.mtr_id=mt.mtr_id and r3.read_seq=3
          left join ci_reg_read rr3 on rr3.mr_id=mr.mr_id and rr3.reg_id=r3.reg_id
      left join ci_mtr_char dpp on dpp.mtr_id=mt.mtr_id and dpp.char_type_cd='DT-P-POV'
      left join ci_mtr_char ppv on ppv.mtr_id=mt.mtr_id and ppv.char_type_cd='PER-POV'
    where 1=1
      and mh.removal_dttm is null
      --
      and mh.sp_id=s_id
    ;
  --
BEGIN
  execute immediate ('create index ${s2}_idx on ${s2}(sp_id)');
  for r0 in c0 loop
    iNum:=1;
    for r1 in c1(r0.sp_id) loop
      insert into ${s3} values(r0.sp_id,r1.item_id,iNum);
      iNum:=iNum+1;
    end loop;
    --
    for r2 in c2(r0.sp_id) loop
      insert into ${s2} 
      values(r0.sp_id,r2.mtr_config_id,r2.mtr_id,r2.serial_nbr,r2.mfg_cd,r2.model_cd,r2.mtr_type_cd
             ,r2.read_dttm,r2.reg_reading1,r2.reg_reading2,r2.reg_reading3,r2.dpp,r2.ppv,r2.install_const,r2.isu);
      iNum:=iNum+1;
    end loop;
    --
    update ${s1}_h t set t.l_done=1 where t.sp_id=r0.sp_id;
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
