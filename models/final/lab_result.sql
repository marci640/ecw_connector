with union_all as (
  select * from {{ ref('int_hl7_labs') }}
  union all
  select * from {{ ref('int_labs') }}
),
ranked as (
  select
    *,
    row_number() over (
      partition by lab_result_id
      order by case when _src = 'int_hl7_labs' then 1 else 2 end
    ) as _rn
  from union_all
)
select
    lab_result_id
  , person_id
  , patient_id
  , encounter_id
  , accession_number
  , source_order_type
  , source_order_code
  , source_order_description
  , source_component_code
  , source_component_type
  , source_component_description
  , normalized_order_type
  , normalized_order_code
  , normalized_order_description
  , normalized_component_code
  , normalized_component_type
  , normalized_component_description
  , status
  , result
  , result_datetime
  , collection_datetime
  , source_units
  , normalized_units
  , source_reference_range_low
  , source_reference_range_high
  , normalized_reference_range_low
  , normalized_reference_range_high
  , source_abnormal_flag
  , normalized_abnormal_flag
  , specimen
  , ordering_practitioner_id
  , data_source
  , file_name
  , ingest_datetime
from ranked
where _rn = 1
