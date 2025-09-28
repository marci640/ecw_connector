select
  cast(null as {{ dbt.type_string() }}) as condition_id,
  cast(null as {{ dbt.type_string() }}) as person_id
  cast(problemlist.patientId as {{ dbt.type_string() }} ) as patient_id,
  cast(problemlist.encounterId as {{ dbt.type_string() }} ) as encounter_id,
  cast(null as {{ dbt.type_string() }}) as claim_id,
  cast(problemlist.AddedDate as date) as recorded_date,
  cast(problemlist.onsetdate as date) as onset_date,
  cast(problemlist.resolvedon as date) as resolved_date,
  cast(problemlist.status as {{ dbt.type_string() }} ) as status,
  cast(problemlist.encounterId as {{ dbt.type_string() }} ) as encounter_id,
  cast('problem' as {{ dbt.type_string() }} ) as condition_type,
  cast('icd-10-cm' as {{ dbt.type_string() }} ) as source_code_type,
  cast(itemdetail.value as {{ dbt.type_string() }} ) as source_code,
  cast(null as {{ dbt.type_string() }} ) as source_description,
  cast(null as {{ dbt.type_string() }} ) as normalized_code_type,
  cast(null as {{ dbt.type_string() }} ) as normalized_code,
  cast(null as {{ dbt.type_string() }} ) as normalized_description,
  cast(null as {{ dbt.type_int() }} ) as condition_rank,
  cast(null as {{ dbt.type_string() }} ) as present_on_admit_code,
  cast(null as {{ dbt.type_string() }} ) as present_on_admit_description,
  cast('ecw'  as {{ dbt.type_string() }} ) as data_source,
  cast(null as {{ dbt.type_string() }} ) as file_name,
  cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime


from 
  {{ref('stg_ecw__problemlist')}} problemlist
  inner join {{ref('stg_ecw__itemdetail')}} itemdetail
    on itemdetail.itemID = problemlist.asmtId

where itemdetail.propid = 13