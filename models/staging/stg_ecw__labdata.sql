with base as (
  select
  {% for column in adapter.get_columns_in_relation(source('ecw','labdata')) %}
    cast(
      nullif(
        trim(
          cast({{ column.name }} as {{ dbt.type_string() }})
        ), ''
      ) as {{ dbt.type_string() }}
    ) as {{ column.name }}{% if not loop.last %},{% endif %}
  {% endfor %}
  from {{ source('ecw','labdata') }}
  where
    deleteFlag = 0
    and cancelled = 0
)

select
    ReportId
  , EncounterId
  , ItemId
  , result
  , received
  , status
  , ResultDate
  , collDate
  , collTime
  , resultime
  , ReviewedDate
  , ReviewedBy
  , ordPhyId
  , priority
  , assignedToId
  , deleteFlag
  , cancelled
from base