with base as (
  select
  {% for column in adapter.get_columns_in_relation(source('ecw','users')) %}
    cast(
      nullif(
        trim(
          cast({{ column.name }} as {{ dbt.type_string() }})
        ), ''
      ) as {{ dbt.type_string() }}
    ) as {{ column.name }}{% if not loop.last %},{% endif %}
  {% endfor %}
  from {{ source('ecw','users') }}
)

select
    dob
  , sex
  , uemail
  , ufname
  , uid
  , ulname
  , umobileno
  , upaddress
  , upaddress2
  , upcity
  , upPhone
  , upstate
  , UserType
  , zipcode
from base