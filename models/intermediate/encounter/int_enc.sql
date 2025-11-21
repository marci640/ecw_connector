select
      cast(enc.encounterID as {{ dbt.type_string() }}) as encounter_id
    , cast(enc.patientID as {{ dbt.type_string() }}) as person_id
    , cast(enc.patientID as {{ dbt.type_string() }}) as patient_id
    , cast('office visit' as {{ dbt.type_string() }}) as encounter_type --TODO: arg for 'telehealth' indentifier (visit type naming convention/coding practice?)
    , cast(enc.date as date) as encounter_start_date
    , cast(enc.date as date) as encounter_end_date
    , cast(null as {{ dbt.type_int() }}) as length_of_stay
    , cast(null as {{ dbt.type_string() }}) as admit_source_code
    , cast(null as {{ dbt.type_string() }}) as admit_source_description
    , cast(null as {{ dbt.type_string() }}) as admit_type_code
    , cast(null as {{ dbt.type_string() }}) as admit_type_description
    , cast(null as {{ dbt.type_string() }}) as discharge_disposition_code
    , cast(null as {{ dbt.type_string() }}) as discharge_disposition_description
    , cast(usersProviders.uid as {{ dbt.type_string() }}) as attending_provider_id -- TODO: going with resourceID, other clinics may use doctorID - confirm
    , cast(usersProviders.uname as {{ dbt.type_string() }}) as attending_provider_name
    , cast(facility.Id as {{ dbt.type_string() }}) as facility_id
    , cast(facility.name as {{ dbt.type_string() }}) as facility_name
    , cast(diagnosis.source_code_type as {{ dbt.type_string() }}) as primary_diagnosis_code_type
    , cast(diagnosis.source_code as {{ dbt.type_string() }}) as primary_diagnosis_code
    , cast(diagnosis.source_description as {{ dbt.type_string() }}) as primary_diagnosis_description
    , cast(null as {{ dbt.type_string() }}) as drg_code_type
    , cast(null as {{ dbt.type_string() }}) as drg_code
    , cast(null as {{ dbt.type_string() }}) as drg_description
    , cast(null as {{ dbt.type_numeric() }}) as paid_amount
    , cast(null as {{ dbt.type_numeric() }}) as allowed_amount
    , cast(null as {{ dbt.type_numeric() }}) as charge_amount
    , cast('ecw' as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from
  {{ ref('stg_ecw__enc') }} enc
    -- filter for CHK appointments only? completed encounters
  inner join {{ ref('stg_ecw__users') }} usersProviders
    on usersProviders.uid = enc.ResourceId
  inner join {{ ref('stg_ecw__edi_facilities') }} facility
    on facility.Id = enc.facilityId
  left join {{ ref('int_enc_diagnosis') }} diagnosis
    on diagnosis.encounter_id = enc.encounterID and diagnosis.condition_rank = 1