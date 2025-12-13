select
      cast(ImmunizationId as {{ dbt.type_string() }}) as immunization_id
    , cast(enc.patientId as {{ dbt.type_string() }}) as person_id
    , cast(enc.patientId as {{ dbt.type_string() }}) as patient_id
    , cast(enc.encounterId as {{ dbt.type_string() }}) as encounter_id
    , cast('cvx' as {{ dbt.type_string() }}) as source_code_type
    , cast(immunizations.cvx_code as {{ dbt.type_string() }}) as source_code
    , cast(immunizations.vaccinename as {{ dbt.type_string() }}) as source_description
    , cast(null as {{ dbt.type_string() }}) as normalized_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_code
    , cast(null as {{ dbt.type_string() }}) as normalized_description
    , cast('completed' as {{ dbt.type_string() }}) as status
    , cast(null as {{ dbt.type_string() }}) as status_reason
    , cast(enc.date as date) as occurrence_date
    , cast(immunizations.dose as {{ dbt.type_string() }}) as source_dose
    , cast(null as {{ dbt.type_string() }}) as normalized_dose
    , cast(immunizations.lotNumber as {{ dbt.type_string() }}) as lot_number
    , cast(vaccine_sites.Location as {{ dbt.type_string() }}) as body_site
    , cast(immunizations.Route as {{ dbt.type_string() }}) as route
    , cast(enc.facilityId as {{ dbt.type_string() }}) as location_id
    , cast(enc.ResourceId as {{ dbt.type_string() }}) as practitioner_id
    , cast('ecw' as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime

from
    {{ ref('stg_ecw__immunizations') }} immunizations
    inner join {{ ref('stg_ecw__immunizationstatus') }} immun_status
      on immun_status.ImmStatusId = immunizations.immstatus
    inner join {{ ref('stg_ecw__enc') }} enc
      on enc.encounterID = immunizations.encounterid
    left join {{ ref('stg_ecw__vaccines_sites') }} vaccine_sites
      on vaccine_sites.Code = immunizations.location

where
  immunizations.deleteFlag != 1 and immun_status.statusdesc = 'Administered'