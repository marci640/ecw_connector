with base as (
  select
  {% for column in adapter.get_columns_in_relation(source('ecw','loinccodes')) %}
    cast(
      nullif(
        trim(
          cast({{ column.name }} as {{ dbt.type_string() }})
        ), ''
      ) as {{ dbt.type_string() }}
    ) as {{ column.name }}{% if not loop.last %},{% endif %}
  {% endfor %}
  from {{ source('ecw','loinccodes') }}
)

select
    id
  , itemid
  , CODE
  , UpdatedBy
  , UpdatedTime
  , LoincType
  , deleteflag
from base