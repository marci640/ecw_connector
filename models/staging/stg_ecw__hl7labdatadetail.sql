with base as (
  select
  {% for column in adapter.get_columns_in_relation(source('ecw','hl7labdatadetail')) %}
    cast(
      nullif(
        trim(
          cast({{ column.name }} as {{ dbt.type_string() }})
        ), ''
      ) as {{ dbt.type_string() }}
    ) as {{ column.name }}{% if not loop.last %},{% endif %}
  {% endfor %}
  from {{ source('ecw','hl7labdatadetail') }}
)

select
    ReportId
  , hl7itemid
  , name
  , value
  , units
  , range
from base