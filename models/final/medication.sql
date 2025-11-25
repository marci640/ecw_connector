select
      cast(ID as {{ dbt.type_string() }}) as medication_id
    , cast(PatientId as {{ dbt.type_string() }}) as person_id
    , cast(PatientId as {{ dbt.type_string() }}) as patient_id
    , cast(EncounterId as {{ dbt.type_string() }}) as encounter_id
    , cast(null as date) as dispensing_date
    , cast(ScriptDate as date) as prescribing_date
    , cast('ndc' as {{ dbt.type_string() }}) as source_code_type
    , cast(ndccode as {{ dbt.type_string() }}) as source_code
    , cast(DrugName as {{ dbt.type_string() }}) as source_description
    , cast(null as {{ dbt.type_string() }}) as ndc_code
    , cast(null as {{ dbt.type_string() }}) as ndc_description
    , cast(null as {{ dbt.type_string() }}) as rxnorm_code
    , cast(null as {{ dbt.type_string() }}) as rxnorm_description
    , cast(null as {{ dbt.type_string() }}) as atc_code
    , cast(null as {{ dbt.type_string() }}) as atc_description
    , cast(Route as {{ dbt.type_string() }}) as route
    , cast(null as {{ dbt.type_string() }}) as strength
    , cast(Quantity as {{ dbt.type_int() }}) as quantity
    , cast(null as {{ dbt.type_string() }}) as quantity_unit
    , cast(null as {{ dbt.type_int() }}) as days_supply
    , cast(doctorId as {{ dbt.type_string() }}) as practitioner_id
    , cast('ecw' as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from
    {{ ref('stg_ecw__rxhub_scriptlog') }}