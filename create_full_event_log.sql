CREATE OR REPLACE TABLE noshad.aim2_event_list_all_v6 as
SELECT 

jc_uid, 
enc_id, 
event_type,  -- ||' - ' || (case when MP.unique_role is not null then MP.unique_role else ' Provider: None ' end) as event_type, 
event_name,
event_time, 
emergencyAdmitTime,  
tpaAdminTime, 
DATETIME_DIFF(CAST(event_time AS DATETIME), emergencyAdmitTime, MINUTE) AS time_diff, 
COALESCE(PM1.prov_type, PM2.unique_role)  as user_type

FROM
(


-- Order Medication Table
(
SELECT OM0.jc_uid as jc_uid, 
  OM0.pat_enc_csn_id_coded as enc_id, 
  'Order Medication' as event_type, --|| OM0.med_description as event_type,
  CAST(OM0.order_med_id_coded AS STRING) as event_id,
  OM0.med_description as event_name,
  OM0.order_time_jittered as event_time,
  cohort.emergencyAdmitTime as emergencyAdmitTime,
  Cohort.tpaAdminTime as tpaAdminTime,
  OM0.authr_prov_map_id as prov_id
  
  FROM `noshad.cohort_v2` as Cohort
  LEFT JOIN `starr_datalake2018.order_med` AS OM0
  ON OM0.pat_enc_csn_id_coded = Cohort.pat_enc_csn_id_coded

) 
 
UNION ALL


-- Order Procedure Table (with order time)
( SELECT OP0.jc_uid as jc_uid, 
  OP0.pat_enc_csn_id_coded as enc_id, 
  'Order Procedure' as event_type, --|| OP0.description as event_type,
  OP0.proc_code as event_id,
  OP0.description as event_name,
  OP0.order_time_jittered as event_time,
  cohort.emergencyAdmitTime as emergencyAdmitTime,
  Cohort.tpaAdminTime as tpaAdminTime,
  OP0.authrzing_prov_map_id as prov_id
  
  FROM `noshad.cohort_v2` as Cohort
  LEFT JOIN `starr_datalake2018.order_proc` AS OP0
  ON OP0.pat_enc_csn_id_coded = Cohort.pat_enc_csn_id_coded
)

UNION ALL

-- Procedure Start Table (with procedure start time)
( SELECT OP0.jc_uid as jc_uid, 
  OP0.pat_enc_csn_id_coded as enc_id, 
  'Initiate Procedure' as event_type, --|| OP0.description as event_type,
  OP0.proc_code as event_id,
  OP0.description as event_name,
  OP0.proc_start_time_jittered as event_time,
  cohort.emergencyAdmitTime as emergencyAdmitTime,
  Cohort.tpaAdminTime as tpaAdminTime,
  OP0.authrzing_prov_map_id as prov_id
  
  FROM `noshad.cohort_v2` as Cohort
  LEFT JOIN `starr_datalake2018.order_proc` AS OP0
  ON OP0.pat_enc_csn_id_coded = Cohort.pat_enc_csn_id_coded
)

UNION ALL

-- MAR Table
(
SELECT MAR0.jc_uid as jc_uid, 
  MAR0.pat_enc_csn_id_coded as enc_id, 
  'Medication Given'  as event_type, --|| OM.med_description   as event_type,
  CAST(MAR0.order_med_id_coded AS STRING) as event_id,
  OM.med_description as event_name,
  MAR0.taken_time_jittered as event_time,
  cohort.emergencyAdmitTime as emergencyAdmitTime,
  Cohort.tpaAdminTime as tpaAdminTime,
  OM.authr_prov_map_id as prov_id
  
  FROM `noshad.cohort_v2` as Cohort
  LEFT JOIN `starr_datalake2018.mar` AS MAR0
  ON MAR0.pat_enc_csn_id_coded = Cohort.pat_enc_csn_id_coded
  LEFT JOIN `starr_datalake2018.order_med` AS OM
  ON OM.order_med_id_coded = MAR0.order_med_id_coded
)

UNION ALL

-- Lab Results Table
(

With DESCR AS 
(
SELECT proc_code, description FROM `som-nero-phi-jonc101.starr_datalake2018.order_proc`
GROUP BY proc_code, description
)

SELECT LR0.rit_uid as jc_uid, 
  LR0.pat_enc_csn_id_coded as enc_id, 
  'Lab Result' as event_type, --|| LR0.proc_code as event_type,
  LR0.proc_code as event_id,
  DESCR.description as event_name, 
  LR0.order_time_jittered as event_time,
  cohort.emergencyAdmitTime as emergencyAdmitTime,
  Cohort.tpaAdminTime as tpaAdminTime,
  LR0.auth_prov_map_id as prov_id
  
  FROM `noshad.cohort_v2` as Cohort
  LEFT JOIN `starr_datalake2018.lab_result` AS LR0
  ON LR0.pat_enc_csn_id_coded = Cohort.pat_enc_csn_id_coded
  
  LEFT JOIN DESCR
  ON LR0.proc_code = DESCR.proc_code
)

UNION ALL


-- Lab Collection Table
(

With DESCR AS 
(
SELECT proc_code, description FROM `som-nero-phi-jonc101.starr_datalake2018.order_proc`
GROUP BY proc_code, description
)

SELECT LR0.rit_uid as jc_uid, 
  LR0.pat_enc_csn_id_coded as enc_id, 
  'Lab Collection' as event_type, -- || LR0.proc_code as event_type,
  LR0.proc_code as event_id,
  DESCR.description as event_name,   --lab_name
  LR0.taken_time_jittered as event_time,
  cohort.emergencyAdmitTime as emergencyAdmitTime,
  Cohort.tpaAdminTime as tpaAdminTime,
  LR0.auth_prov_map_id as prov_id
  
  FROM `noshad.cohort_v2` as Cohort
  LEFT JOIN `starr_datalake2018.lab_result` AS LR0
  ON LR0.pat_enc_csn_id_coded = Cohort.pat_enc_csn_id_coded
  
    
  LEFT JOIN DESCR
  ON LR0.proc_code = DESCR.proc_code
  
)

UNION ALL

-- Radiology Procedure Started
(
SELECT RAD.jc_uid as jc_uid, 
  RAD.pat_enc_csn_id_coded as enc_id, 
  'Radiology Proc Started' as event_type,
  RAD.proc_code as event_id,
  RAD.description as event_name,
  RAD.proc_start_time_jittered as event_time,
  cohort.emergencyAdmitTime as emergencyAdmitTime,
  Cohort.tpaAdminTime as tpaAdminTime,
  RAD.authrzing_prov_map_id as prov_id
  FROM `noshad.cohort_v2` as Cohort
  LEFT JOIN `starr_datalake2018.radiology_report_meta` AS RAD
  ON RAD.pat_enc_csn_id_coded = Cohort.pat_enc_csn_id_coded
)

UNION ALL

-- Radiology Report
(
SELECT RAD.jc_uid as jc_uid, 
  RAD.pat_enc_csn_id_coded as enc_id, 
  'Radiology Report' as event_type,
  RAD.proc_code as event_id,
  RAD.description as event_name,
  RAD.rpt_prelim_dttm_jittered as event_time,
  cohort.emergencyAdmitTime as emergencyAdmitTime,
  Cohort.tpaAdminTime as tpaAdminTime,
  RAD.authrzing_prov_map_id as prov_id
  FROM `noshad.cohort_v2` as Cohort
  LEFT JOIN `starr_datalake2018.radiology_report_meta` AS RAD
  ON RAD.pat_enc_csn_id_coded = Cohort.pat_enc_csn_id_coded
)

UNION ALL


-- Specialist Consultation
(
SELECT Con.jc_uid as jc_uid, 
  Con.pat_enc_csn_id_coded as enc_id, 
  'Consultation Note' as event_type,
  CAST(Con.effective_dept_id AS STRING) as event_id,
  DEP.department_name as event_name,
  Con.note_date_jittered as event_time,
  cohort.emergencyAdmitTime as emergencyAdmitTime,
  Cohort.tpaAdminTime as tpaAdminTime,
  Con.author_prov_map_id as prov_id
  FROM `noshad.cohort_v2` as Cohort
  LEFT JOIN `starr_datalake2018.clinical_doc_meta` AS Con
    ON Con.pat_enc_csn_id_coded = Cohort.pat_enc_csn_id_coded
  LEFT JOIN `starr_datalake2018.dep_map` as DEP
    ON Con.effective_dept_id = DEP.department_id
  WHERE Con.note_type LIKE '%Consultation%'
  
)

UNION ALL

-- Access Log Data
(select 

  al.rit_uid as jc_uid, 
  cohort.pat_enc_csn_id_coded as enc_id, 
  'Access log' as event_type, --|| metric_name as event_type,
  CAST(al.metric_id AS STRING) as event_id,
  CAST(al.metric_name AS STRING) as event_name,
  CAST(al.access_time_jittered AS DATETIME) as event_time,
  cohort.emergencyAdmitTime as emergencyAdmitTime,
  Cohort.tpaAdminTime as tpaAdminTime,
  al.user_deid as prov_id

  from noshad.cohort_v2 as cohort
  join `shc_access_log.shc_access_log_de` as al on cohort.jc_uid  = al.rit_uid 
  -- only capture the access logs within 60 min before and after the cohort
where  datetime_diff(al.access_time_jittered, cohort.tpaAdminTime, MINUTE) >= -360 --up to 6 hours before tpa admin time 
  and datetime_diff(al.access_time_jittered, cohort.tpaAdminTime, MINUTE) <= 0
)



UNION ALL

-- Admission from Access Log Data
(select 

  al.rit_uid as jc_uid, 
  cohort.pat_enc_csn_id_coded as enc_id, 
  'Access log' as event_type, --|| metric_name as event_type,
  CAST(al.metric_id AS STRING) as event_id,
  CAST(al.metric_name AS STRING) as event_name,
  CAST(al.access_time_jittered AS DATETIME) as event_time,
  cohort.emergencyAdmitTime as emergencyAdmitTime,
  Cohort.tpaAdminTime as tpaAdminTime,
  al.user_deid as prov_id

  from noshad.cohort_v2 as cohort
  join `shc_access_log.shc_access_log_de` as al on cohort.jc_uid  = al.rit_uid 
  -- only capture the access logs within 60 min before and after the cohort
where  datetime_diff(al.access_time_jittered, cohort.emergencyAdmitTime, MINUTE) >= -20 --up to  20 min before the admit time 
  and datetime_diff(al.access_time_jittered, cohort.emergencyAdmitTime, MINUTE) <= 0
  and CAST(al.metric_name AS STRING) LIKE 'Registration/ADT workflow initiated'
)


) AS EV

-- JOIN WITH PROV_TYPE MAPPING 
LEFT JOIN `starr_datalake2018.prov_map` as PM1 ON PM1.prov_map_id=EV.prov_id
LEFT JOIN `noshad.prov_id_map_2` as PM2 ON PM2.prov_map_id=EV.prov_id

WHERE CAST(event_time AS DATETIME) <= tpaAdminTime
GROUP BY jc_uid, enc_id, event_type, event_name, event_time, emergencyAdmitTime, time_diff, tpaAdminTime, user_type
ORDER BY jc_uid, enc_id, event_time
