select
      cast(vitals.Id as {{ dbt.type_string() }}) as observation_id
    , cast(enc.patientID as {{ dbt.type_string() }}) as person_id
    , cast(enc.patientID as {{ dbt.type_string() }}) as patient_id
    , cast(vitals.encounterID as {{ dbt.type_string() }}) as encounter_id
    , cast(null as {{ dbt.type_string() }}) as panel_id
    , cast(enc.date as date) as observation_date
    , cast('vital-signs' as {{ dbt.type_string() }}) as observation_type
    , cast('loinc' as {{ dbt.type_string() }}) as source_code_type
    , cast(loinc.CODE as {{ dbt.type_string() }}) as source_code
    , cast(vitaltypes.type as {{ dbt.type_string() }}) as source_description
    , cast(null as {{ dbt.type_string() }}) as normalized_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_code
    , cast(null as {{ dbt.type_string() }}) as normalized_description
    , cast(vitals.value as {{ dbt.type_string() }}) as result
    , cast(vitaltypes.displayUom as {{ dbt.type_string() }}) as source_units
    , cast(null as {{ dbt.type_string() }}) as normalized_units
    , cast(vitalrange.lowRange as {{ dbt.type_string() }}) as source_reference_range_low
    , cast(vitalrange.highRange as {{ dbt.type_string() }}) as source_reference_range_high
    , cast(null as {{ dbt.type_string() }}) as normalized_reference_range_low
    , cast(null as {{ dbt.type_string() }}) as normalized_reference_range_high
    , cast('ecw' as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from
    {{ ref('stg_ecw__vitals') }} vitals
    left join {{ ref('stg_ecw__vitaltypes') }} vitaltypes
        on vitaltypes.itemId = vitals.vitalID
    left join {{ ref('stg_ecw__items') }} items
        on items.itemId = vitaltypes.vitalID
    left join {{ ref('stg_ecw__enc') }} enc
        on enc.encounterID = vitals.encounterID
    left join {{ ref('stg_ecw__loinccodes') }} loinc
        on loinc.itemid = items.itemId
    left join {{ ref('stg_ecw__vitalrange') }} vitalrange
        on vitalrange.vitalTypeId = vitaltypes.itemId;