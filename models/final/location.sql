select
      fac.Id as location_id,
      fac.NPI as npi,
      fac.name as name,
      fac.PracticeType as facility_type,
      fac.PayableTo as parent_organization,
      (COALESCE(fac.AddressLine1,'') || ' ' || COALESCE(fac.AddressLine2,'')) as address,
      fac.City as city,
      fac.State as state,
      fac.Zip as zip_code,
      cast(null as {{ dbt.type_float() }}) as latitude,
      cast(null as {{ dbt.type_float() }}) as longitude,
      cast('ecw'  as {{ dbt.type_string() }} ) as data_source,
      cast(null as {{ dbt.type_string() }} ) as file_name,
      cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
from {{ ref('stg_ecw__edi_facilities') }} fac