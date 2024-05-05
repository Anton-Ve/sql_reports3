COLUMN TASK_SEQ NEW_V TASK_SEQ_CURRENT
select (&TASK_SEQ_CURRENT.+1) TASK_SEQ from dual;

PROMPT VZ_TASK_BASE_LINE.&TASK_CAT..&TASK_CD..&TASK_SEQ_CURRENT..merge - start
declare
   l_seq        number := &TASK_SEQ_CURRENT;
   l_task_cd    VARCHAR2(16) := '&TASK_CD.';
   l_cat_cd     VARCHAR2(16) := '&TASK_CAT.';
   l_ENTITY_NM  VARCHAR2(32) := '';
   l_SHEET_NM   VARCHAR2(32) := 'Список';
   l_SHEET_COL  VARCHAR2(32) := '';
   l_SQL_OP     VARCHAR2(7)  := 'NONE';
   l_ENABLE     number := 1;
   l_EXPORT	    number := 1;
   l_LINE_BUILD number := 2;
   l_DESCR      clob := Q'##';
   l_SQL_HEADER clob := Q'#select 'ACCT_ID' key, 'ИД Счета CC' nm
from dual
union all
select 'KVIT' key, 'Квитанция' nm
from dual
union all
select 'SA_ID' key, 'ИД РДО' nm
from dual
union all
select 'VR' key, 'Вариант расчетов Сбыт-УК внутри МКЖД' nm
from dual
union all
select 'COUNTY' key, 'Район области' nm
from dual
union all
select 'GEO_CODE' key, 'Тип нас.пункта' nm
from dual
union all
select 'CITY' key, 'Населенный пункт' nm
from dual
union all
select 'NP2' key, 'Населенный пункт 2' nm
from dual
union all
select 'UL_TYPE' key, 'Тип улицы' nm
from dual
union all
select 'UL' key, 'Улица' nm
from dual
union all
select 'DOM' key, 'Номер дома' nm
from dual
union all
select 'KORP' key, 'Номер корпуса и литера' nm
from dual
union all
select 'KV' key, 'Номер квартиры и литера' nm
from dual
union all
select 'PREM_TYPE' key, 'Тип объекта обслуживания ЛС' nm
from dual
union all
select 'FIAS' key, 'ФИАС' nm
from dual
union all
select 'PRNT_PREM_TYPE' key, 'ТИП родительского объекта' nm
from dual
union all
select 'PNAME' key, 'Наименование субъекта' nm
from dual
union all
select 'PH' key, 'Контакты (телефон)' nm 
from dual 
union all
select 'SERIAL_NBR' key, 'Номер ПУ' nm
from dual
union all
select 'MDL' key, 'Тип эл.счетчика (модель)' nm
from dual
union all
select 'MTR_TYPE' key, 'Тип ПУ' nm
from dual
union all
select 'INSTALL_DT' key, 'Дата установки ПУ' nm
from dual
union all
select 'REG_READING1' key, 'Начальные показания ПУ рег.1' nm
from dual
union all
select 'REG_READING2' key, 'Начальные показания ПУ рег.2' nm
from dual
union all
select 'REG_READING3' key, 'Начальные показания ПУ рег.3' nm
from dual
union all
select 'DPP' key, 'Дата последней поверки ПУ' nm
from dual
union all
select 'PPV' key, 'Периодичность поверки ПУ' nm
from dual
union all
select 'INSTALL_CONST' key, 'Коэффициент трансформации' nm
from dual
union all
select 'SAS_ACT' key, 'Связь РДО/ТУ активна' nm
from dual 
union all
select 'TR1_INSTALL_DT' key, 'Дата установки ТТ (Фаза А)' nm
from dual 
union all
select 'TR1_DPP' key, 'Дата последней поверки ТТ (Фаза А)' nm
from dual 
union all
select 'TR1_PPV' key, 'Периодичность поверки ТТ (фаза А)' nm
from dual 
union all
select 'SERIAL_NBR_OLD' key, 'Номер снятого ПУ' nm
from dual
union all
select 'REMOVAL_DTTM' key, 'Дата снятия ПУ' nm
from dual  
union all
select 'MODEL_OLD' key, 'Модель снятого ПУ' nm
from dual  
union all
select 'IKU' key, 'Управляющая компания' nm
from dual
union all
select 'SOP' key, 'На какую ТСО относится полезный отпуск' nm
from dual
union all
select 'TRN' key, 'На какую ТСО относится полезный отпуск при расчете потерь' nm
from dual
union all
select 'ONR' key, 'К чьим сетям подключена ТУ' nm
from dual
union all
select 'ONR2' key, 'Владелец сетей на 1 ступень выше' nm
from dual
union all
select 'N_DOG' key, '№ Договора' nm
from dual
union all
select 'ISU' key, 'ПУ присоединен к ИСУ?' nm
from dual   
   #';
   l_SQL_DATA  clob := Q'##';
   l_SQL_QUERY   clob := Q'#select distinct
  tt.acct_id
  ,uch.descr "Участок"
  ,coalesce(tt.kvit,tt.acct_id) kvit
  ,tt.sa_id
  ,vr.descr vr
  ,pr.county
  ,pr.geo_code
  ,pr.city
  ,pr.address4 np2
  ,ut.descr ul_type
  ,pr.address3 ul
  ,pr.address2 dom
  ,pr.num2 korp
  ,pr.num1 kv
  ,pt.descr prem_type
  ,max(fias.adhoc_char_val) keep (dense_rank last order by fias.effdt) over (partition by fias.prem_id) fias
  ,case when tt.prnt_prem_type_cd='MNOGOKV' then ppt.descr end prnt_prem_type
  ,max(pn.entity_name) keep (dense_rank last order by pn.seq_num) over (partition by pn.per_id) pname
  ,(select listagg(trim(pp.phone),',') within group (order by pp.seq_num) from ci_per_phone pp where per_id=tt.per_id) ph
  ,pu.serial_nbr
  ,mdl.descr mdl
  ,mtt.descr mtr_type
  ,pu.install_dt
  ,pu.reg_reading1
  ,pu.reg_reading2
  ,pu.reg_reading3
  ,pu.dpp
  ,pu.ppv
  ,pu.install_const
  ,case when tt.sas_act=1 then 'да' else 'нет' end sas_act
  ,tr1.install_dt tr1_install_dt
  ,tr1.dpp tr1_dpp
  ,tr1.ppv tr1_ppv
  ,tt.serial_nbr_old
  ,tt.removal_dttm
  ,tt.model_old
  ,iku.descr iku
  ,sop_n.descr50 sop
  ,trn_n.descr50 trn
  ,onr_n.descr50 onr
  ,onr2_n.descr50 onr2
  ,tt.n_dog
  ,pu.isu
from
   ${s1}_h tt
  join ci_char_val_l uch on uch.char_type_cd='U4ASTOK' and uch.char_val=tt.uch and uch.language_cd='RUS'
  left join ci_char_val_l vr on vr.char_type_cd='VR-UK-SB' and vr.char_val=tt.vr and vr.language_cd='RUS'
  join ci_prem pr on pr.prem_id=tt.prem_id
    join ci_prem_type_l pt on pt.prem_type_cd=tt.prem_type_cd and pt.language_cd='RUS'
    left join ci_prem_char fias on fias.prem_id=tt.prem_id and fias.char_type_cd='FIAS'
    left join ci_lookup_val_l ut on ut.field_name='HOUSE_TYPE' and ut.field_value=pr.house_type and ut.language_cd='RUS'
    left join ci_prem_type_l ppt on ppt.prem_type_cd=tt.prnt_prem_type_cd and ppt.language_cd='RUS'
  join ci_per_name pn on pn.per_id=tt.per_id and pn.name_type_flg='PRIM'
  left join ${s4} tr1 on tr1.sp_id=tt.sp_id
  left join ${s2} pu on pu.sp_id=tt.sp_id
    left join ci_model_l mdl on mdl.mfg_cd=pu.mfg_cd and mdl.model_cd=pu.model_cd and mdl.language_cd='RUS'
    left join ci_mtr_type_l mtt on mtt.mtr_type_cd=pu.mtr_type_cd and mtt.language_cd='RUS'
  left join ci_char_val_l iku on iku.char_type_cd='ISP-KOMU' and iku.char_val=tt.iku and iku.language_cd='RUS'
  left join ci_spr_l sop_n on trim(sop_n.spr_cd)=trim(tt.sop) and sop_n.language_cd='RUS'
  left join ci_spr_l trn_n on trim(trn_n.spr_cd)=trim(tt.trn) and trn_n.language_cd='RUS'
  left join ci_spr_l onr_n on trim(onr_n.spr_cd)=trim(tt.onr) and onr_n.language_cd='RUS'
   left join ci_spr_l onr2_n on trim(onr2_n.spr_cd)=trim(tt.onr2) and onr2_n.language_cd='RUS'
   #';
   l_DB_DATA    VARCHAR2(32) := '';
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
            , l_DB_DATA    DB_DATA
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
                 , tgt.DB_DATA         = src.DB_DATA
        WHERE tgt.LINE_BUILD < src.LINE_BUILD
	WHEN NOT MATCHED THEN
		 INSERT( tgt.TASK_IDX, tgt.SEQ, tgt.ENTITY_NM, tgt.SHEET_NM, tgt.SHEET_COL, tgt.SQL_OP, tgt.ENABLE, tgt.EXPORT, tgt.LINE_BUILD, tgt.DESCR, tgt.SQL_HEADER, tgt.SQL_QUERY, tgt.SQL_DATA, tgt.DB_DATA)  
		 VALUES( src.TASK_IDX, src.SEQ, src.ENTITY_NM, src.SHEET_NM, src.SHEET_COL, src.SQL_OP, src.ENABLE, src.EXPORT, src.LINE_BUILD, src.DESCR, src.SQL_HEADER, src.SQL_QUERY, src.SQL_DATA, src.DB_DATA)  
	;
	commit;
end;
/
