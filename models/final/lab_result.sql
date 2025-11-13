WITH union_all AS (
  SELECT * FROM {{ ref('int_hl7_labs') }}
  UNION ALL
  SELECT * FROM {{ ref('int_labs') }}
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY lab_result_id
      ORDER BY CASE WHEN _src = 'int_hl7_labs' THEN 1 ELSE 2 END
    ) AS _rn
  FROM union_all
)
SELECT
  lab_result_id,
  person_id,
  patient_id,
  encounter_id,
  accession_number,
  source_order_type,
  source_order_code,
  source_order_description,
  source_component_code,
  source_component_type,
  source_component_description,
  normalized_order_type,
  normalized_order_code,
  normalized_order_description,
  normalized_component_code,
  normalized_component_type,
  normalized_component_description,
  status,
  result,
  result_datetime,
  collection_datetime,
  source_units,
  normalized_units
  source_reference_range_low,
  source_reference_range_high,
  normalized_reference_range_low,
  normalized_reference_range_high,
  source_abnormal_flag,
  normalized_abnormal_flag,
  specimen,
  ordering_practitioner_id,
  data_source,
  file_name,
  ingest_datetime
FROM ranked
WHERE _rn = 1;
