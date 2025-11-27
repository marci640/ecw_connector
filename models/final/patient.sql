select 
    cast(patients.pid as {{ dbt.type_string() }}) as person_id
    , cast(patients.pid as {{ dbt.type_string() }}) as patient_id
    , cast(users.suffix as {{ dbt.type_string() }}) as name_suffix
    , cast(users.ufname as {{ dbt.type_string() }}) as first_name
    , cast(users.uminitial as {{ dbt.type_string() }}) as middle_name
    , cast(users.ulname as {{ dbt.type_string() }}) as last_name
    , cast(users.uemail as {{ dbt.type_string() }}) as email
    , cast(users.sex as {{ dbt.type_string() }}) as sex
    , cast(
        case patients.race
          when 'American Indian' then 'american indian or alaska native'
          when 'Asian' then 'asian'
          when 'Black or African American' then 'black or african american'
          when 'Native Hawaiian' then 'native hawaiian or other pacific islander'
          when 'White' then 'white'
          when 'Other Race' then 'other race'
          when 'Unreported/Refused to Report' then 'asked but unknown'
          when 'Hispanic' then 'other race'
          else 'unknown'
        end
      as {{ dbt.type_string() }}) as race
    , cast(
        case ethnicity.Name
          when 'Refused to Report'        then 'asked but unknown'
          when 'Hispanic or Latino'      then 'Hispanic or Latino'
          when 'Not Hispanic or Latino'  then 'Not Hispanic or Latino'
          when 'Uknown'                  then 'unknown'
          else 'unknown'
        end
      as {{ dbt.type_string() }}) as ethnicity
    , cast(users.dob as date) as birth_date
    , cast(patients.deceasedDate as date) as death_date
    , cast(case when patients.deceased = '1' then true else false end as {{ dbt.type_boolean() }}) as death_flag
    , cast(null as {{ dbt.type_string() }}) as social_security_number
    , cast(users.upaddress as {{ dbt.type_string() }}) as address
    , cast(users.upcity as {{ dbt.type_string() }}) as city
    , cast(users.upstate as {{ dbt.type_string() }}) as state
    , cast(users.zipcode as {{ dbt.type_string() }}) as zip_code
    , cast(users.county as {{ dbt.type_string() }}) as county
    , cast(null as {{ dbt.type_float() }}) as latitude
    , cast(null as {{ dbt.type_float() }}) as longitude
    , cast(users.upPhone as {{ dbt.type_string() }}) as phone
    , cast('ecw' as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime

from {{ ref('stg_ecw__patients') }} patients
    inner join {{ ref('stg_ecw__users')  }} users
    on patients.pid = users.uid
    left join {{ ref('stg_ecw__ethnicity') }} ethnicity
    on patients.ethnicity = ethnicity.EthId