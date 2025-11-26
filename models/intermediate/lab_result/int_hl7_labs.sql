select
  cast(concat(labdata.ReportId, '-', hl7.hl7itemid) as {{ dbt.type_string() }}) as lab_result_id
, cast(enc.patientID as {{ dbt.type_string() }}) as person_id
, cast(enc.patientID as {{ dbt.type_string() }}) as patient_id
, cast(labdata.EncounterId as {{ dbt.type_string() }}) as encounter_id
, cast(labdata.ReportId as {{ dbt.type_string() }}) as accession_number
, cast(case when labcptmap.cptcode is not null then 'hcpcs' else 'loinc' end as {{ dbt.type_string() }}) as source_order_type
, cast(coalesce(labcptmap.cptcode, labloinc_order_hl7.code) as {{ dbt.type_string() }}) as source_order_code
, cast(items.itemName as {{ dbt.type_string() }}) as source_order_description
, cast(labloinc_comp_hl7.code as {{ dbt.type_string() }}) as source_component_code
, cast('loinc' as {{ dbt.type_string() }}) as source_component_type
, cast(hl7.name as {{ dbt.type_string() }}) as source_component_description
, cast(null as {{ dbt.type_string() }}) as normalized_order_type
, cast(null as {{ dbt.type_string() }}) as normalized_order_code
, cast(null as {{ dbt.type_string() }}) as normalized_order_description
, cast(null as {{ dbt.type_string() }}) as normalized_component_code
, cast(null as {{ dbt.type_string() }}) as normalized_component_type
, cast(null as {{ dbt.type_string() }}) as normalized_component_description
, cast(labdata.status as {{ dbt.type_string() }}) as status
, cast(coalesce(hl7.value, labdata.result) as {{ dbt.type_string() }}) as result
, cast(concat(labdata.ResultDate, ' ', labdata.resultime) as {{ dbt.type_timestamp() }}) as result_datetime
, cast(concat(labdata.collDate,  ' ', labdata.collTime)  as {{ dbt.type_timestamp() }}) as collection_datetime
, cast(hl7.units as {{ dbt.type_string() }}) as source_units
, cast(null as {{ dbt.type_string() }}) as normalized_units
, cast(
    nullif(split_part(hl7.range, '-', 1), '')
  as {{ dbt.type_string() }}) as source_reference_range_low
, cast(
    nullif(split_part(hl7.range, '-', 2), '')
  as {{ dbt.type_string() }}) as source_reference_range_high
, cast(null as {{ dbt.type_string() }}) as normalized_reference_range_low
, cast(null as {{ dbt.type_string() }}) as normalized_reference_range_high
, cast(null as {{ dbt.type_int() }}) as source_abnormal_flag
, cast(null as {{ dbt.type_int() }}) as normalized_abnormal_flag
, cast(null as {{ dbt.type_string() }}) as specimen
, cast(labdata.ordPhyId as {{ dbt.type_string() }}) as ordering_practitioner_id
, cast('ecw' as {{ dbt.type_string() }}) as data_source
, cast(null as {{ dbt.type_string() }}) as file_name
, cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
, cast('hl7_labs' as {{ dbt.type_string() }}) as _src
from
  {{ ref('stg_ecw__hl7labdatadetail') }} hl7
left join {{ ref('stg_ecw__labdata') }} labdata
  on labdata.ReportId = hl7.ReportId
left join {{ ref('stg_ecw__enc') }} enc
  on enc.encounterID = labdata.EncounterId
left join {{ ref('stg_ecw__items') }} items
  on items.itemid = labdata.itemid
left join {{ ref('stg_ecw__labcptmap') }} labcptmap
  on labcptmap.labCode = labdata.ItemId
left join {{ ref('stg_ecw__labloinccodes') }} labloinc_order_hl7
  on labloinc_order_hl7.itemid = labdata.ItemId
left join {{ ref('stg_ecw__labloinccodes') }} labloinc_comp_hl7
  on labloinc_comp_hl7.itemid = hl7.hl7itemid