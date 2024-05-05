COLUMN TASK_SEQ NEW_V TASK_SEQ_CURRENT
select (&TASK_SEQ_CURRENT.+1) TASK_SEQ from dual;

PROMPT VZ_TASK_BASE_LINE.&TASK_CAT..&TASK_CD..&TASK_SEQ_CURRENT..merge - start
declare
   l_seq        number := &TASK_SEQ_CURRENT;
   l_task_cd    VARCHAR2(16) := '&TASK_CD.';
   l_cat_cd     VARCHAR2(16) := '&TASK_CAT.';
   l_ENTITY_NM  VARCHAR2(32) := 's1';
   l_SHEET_NM   VARCHAR2(32) := '';
   l_SHEET_COL  VARCHAR2(32) := '';
   l_SQL_OP     VARCHAR2(7)  := 'CREATE';
   l_ENABLE     number := 1;
   l_EXPORT	    number := 0;
   l_LINE_BUILD number := 1;
   l_DESCR      clob := Q'##';
   l_SQL_HEADER clob := Q'##';
   l_SQL_QUERY  clob := Q'##';
   l_SQL_DATA   clob := Q'#with temp as (
select distinct 
  a.acct_id 
  ,(select max(ac.adhoc_char_val) keep(dense_rank last order by ac.effdt)  
     from ci_acct_char ac 
      where 1=1
       and ac.acct_id=a.acct_id
         and ac.char_type_cd='N_DOG') n_dog
		 
  ,cast(max(uch.char_val) keep (dense_rank last order by uch.effdt) over (partition by uch.acct_id) as char(16)) uch
  ,ssv.kvit
  ,sa.sa_id
  ,cast(max(vr.char_val) keep (dense_rank last order by vr.effdt) over (partition by vr.sa_id) as char(16)) vr
  ,sp.sp_id
  ,case when sas.stop_dttm is null then 1 else 0 end sas_act
  ,pr.prem_id
  ,pr.prem_type_cd
  ,ppr.prem_id prnt_prem_id
  ,ppr.prem_type_cd prnt_prem_type_cd
  ,case when ppr2.prem_type_cd in ('MNOGOKV','DOM_BLOK') then ppr2.prem_id
        else case when ppr.prem_type_cd in ('MNOGOKV','DOM_BLOK') then  ppr.prem_id else '' end
        end mkd
  ,case when ppr2.prem_type_cd in ('MNOGOKV','DOM_BLOK') then ppr2.prem_type_cd 
        else case when ppr.prem_type_cd in ('MNOGOKV','DOM_BLOK') then  ppr.prem_type_cd else '' end
        end mkd_type
  ,ap.per_id
from
  ci_acct a
    join ci_acct_char uch on uch.acct_id=a.acct_id and uch.char_type_cd='U4ASTOK'
  left join (select
               cast(s.descr50 as char(10)) kvit
               ,d.acct_id
             from 
               ci_stm_cnst s
               join ci_stm_cnst_dtl d on d.stm_cnst_id=s.stm_cnst_id and d.end_dt is null
             where
               s.eff_status='A'
             ) ssv on ssv.acct_id=a.acct_id
  join ci_acct_per ap on ap.acct_id=a.acct_id and ap.main_cust_sw='Y'
  join ci_sa sa on sa.acct_id=a.acct_id and sa.sa_status_flg='20'
                and exists (select 1 from ci_sa_type st where st.cis_division=sa.cis_division and st.sa_type_cd=sa.sa_type_cd and st.svc_type_cd='99')
                and exists (select 1 from ci_sa_type_char stc where 1=1
                            and stc.cis_division=sa.cis_division and stc.sa_type_cd=sa.sa_type_cd
                            and stc.char_type_cd='TIP-RDO' and stc.char_val='USL')
                and not exists (select 1 from ci_sa_type_char stc where 1=1
                                and stc.cis_division=sa.cis_division and stc.sa_type_cd=sa.sa_type_cd
                                and stc.char_type_cd='SA-HIGH' and stc.char_val='DA')
    left join ci_sa_char vr on vr.sa_id=sa.sa_id and vr.char_type_cd='VR-UK-SB'
  join ci_sa_sp sas on sas.sa_id=sa.sa_id and (sas.stop_dttm is null or a.protect_cyc_sw='Y')
                    and sas.start_dttm=(select max(start_dttm) from ci_sa_sp where sa_id=sas.sa_id and sp_id=sas.sp_id)
  join ci_sp sp on sp.sp_id=sas.sp_id
                and exists (select 1 from ci_sp_type spt where spt.sp_type_cd=sp.sp_type_cd and spt.sp_subtype_flg='M')
  join ci_prem pr on pr.prem_id=sp.prem_id
    left join ci_prem ppr on ppr.prem_id=pr.prnt_prem_id
      left join ci_prem ppr2 on ppr2.prem_id=ppr.prnt_prem_id
where 1=1
   and a.cis_division in (select CAST (Trim(r.VL) as char(5)) cis_division from TABLE( VZ_REPORT_API.split('${PARAMETER_01}'))r)
)
select distinct
  t.*
  ,cast(max(iku.char_val) keep (dense_rank last order by iku.effdt) over (partition by iku.prem_id) as char(16)) iku
  ,cast(max(sop.char_val_fk1) keep (dense_rank last order by sop.effdt) over (partition by sop.sa_id) as char(12)) sop
  ,cast(max(trn.spr_cd) keep (dense_rank last order by trn.effdt) over (partition by trn.sa_id) as char(12)) trn
  ,cast(max(onr.char_val_fk1) keep (dense_rank last order by onr.effdt) over (partition by onr.sa_id) as char(12)) onr
  ,cast(max(onr2.char_val_fk1) keep (dense_rank last order by onr2.effdt) over (partition by onr2.sa_id) as char(12)) onr2
  ,cast(null as varchar2(16)) serial_nbr_old
  ,cast(null as varchar2(250)) model_old 
  ,cast(null as date) removal_dttm
  ,cast(null as number(1)) l_done
 
from
  temp t
  left join ci_prem_char iku on iku.prem_id=t.mkd and iku.char_type_cd='ISP-KOMU'
  left join ci_sa_char sop on sop.sa_id=t.sa_id and sop.char_type_cd='SOPO'
  left join ci_sa_rel trn on trn.sa_id=t.sa_id and trn.sa_rel_type_cd='TRANSP' and trn.sa_rel_status_flg='A'
  left join ci_sa_char onr on onr.sa_id=t.sa_id and onr.char_type_cd='OWNER'
  left join ci_sa_char onr2 on onr2.sa_id=t.sa_id and onr2.char_type_cd='OWNER2'
  where 1=1
  --and ${FILTER_FOR_TEST}
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
