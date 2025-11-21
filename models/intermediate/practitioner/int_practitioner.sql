select
    cast(doctors.doctorID as {{ dbt.type_string() }}) as practitioner_id
    , cast(doctors.NPI as {{ dbt.type_string() }}) as npi
    , cast(usersProviders.ufname as {{ dbt.type_string() }}) as first_name
    , cast(usersProviders.ulname as {{ dbt.type_string() }}) as last_name
    , cast(null as {{ dbt.type_string() }}) as practice_affiliation
    , cast(doctors.speciality as {{ dbt.type_string() }}) as specialty
    , cast(null as {{ dbt.type_string() }}) as sub_specialty
    , cast('ecw' as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime

from {{ ref('stg_ecw__doctors') }} doctors
    inner join {{ ref('stg_ecw__users')  }} usersProviders
    on doctors.doctorID = usersProviders.uid
