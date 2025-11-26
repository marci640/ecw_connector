select
      cast(Id as {{ dbt.type_string() }}) as location_id
    , cast(NPI as {{ dbt.type_string() }}) as npi
    , cast(name as {{ dbt.type_string() }}) as name
    , cast(PracticeType as {{ dbt.type_string() }}) as facility_type
    , cast(PayableTo as {{ dbt.type_string() }}) as parent_organization
    , cast(AddressLine1 as {{ dbt.type_string() }}) as address
    , cast(City as {{ dbt.type_string() }}) as city
    , cast(State as {{ dbt.type_string() }}) as state
    , cast(Zip as {{ dbt.type_string() }}) as zip_code
    , cast(null as {{ dbt.type_float() }}) as latitude
    , cast(null as {{ dbt.type_float() }}) as longitude
    , cast('ecw' as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from
    {{ ref('stg_ecw__edi_facilities') }}