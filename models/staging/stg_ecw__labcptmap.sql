with base as (
  select
  {% for column in adapter.get_columns_in_relation(source('ecw','labcptmap')) %}
    cast(
      nullif(
        trim(
          cast({{ column.name }} as {{ dbt.type_string() }})
        ), ''
      ) as {{ dbt.type_string() }}
    ) as {{ column.name }}{% if not loop.last %},{% endif %}
  {% endfor %}
  from {{ source('ecw','labcptmap') }}
)

select
    labcode
  , cptcode
from (
    select
        labcode
      , cptcode
      , row_number() over (partition by labcode order by cptcode) as rn
    from
        base
) t
where
    rn = 1

-- one lab order can map to multiple cpt codes; pick the first