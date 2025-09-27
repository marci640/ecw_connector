select
  cast(null as {{ dbt.type_string() }}) as condition_id,
  cast(null as {{ dbt.type_string() }}) as person_id
  cast(enc.patientId as {{ dbt.type_string() }} ) as patient_id,
  cast(enc.encounterId as {{ dbt.type_string() }} ) as encounter_id,
  cast(enc.invoiceId as {{ dbt.type_string() }} ) as claim_id,
  cast(enc.date as date) as recorded_date,
  cast(enc.date as date) as onset_date,
  cast(null as date) as resolved_date,
  cast('active' as {{ dbt.type_string() }}) as status,
  cast('encounter' as {{ dbt.type_string() }} ) as condition_type,
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
  {{ref('stg_ecw__diagnosis')}} diagnosis
  inner join {{ref('stg_ecw__enc')}} enc
    on enc.encounterID = diagnosis.EncounterId
  inner join {{ref('stg_ecw__itemdetail')}} itemdetail
    on itemdetail.itemID = diagnosis.ItemId

where itemdetail.propid = 13