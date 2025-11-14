select
    cast(concat(labdata.ReportId, '-', labdatadetail.PropId) as {{ dbt.type_string() }}) as lab_result_id
  , cast(enc.patientID as {{ dbt.type_string() }}) as person_id
  , cast(enc.patientID as {{ dbt.type_string() }}) as patient_id
  , cast(labdata.EncounterId as {{ dbt.type_string() }}) as encounter_id
  , cast(labdata.ReportId as {{ dbt.type_string() }}) as accession_number
  , cast(case when labcptmap.cptcode is not null then 'hcpcs' else 'loinc' end as {{ dbt.type_string() }}) as source_order_type
  , cast(coalesce(labcptmap.cptcode, labloinc_order.code) as {{ dbt.type_string() }}) as source_order_code
  , cast(items.itemName as {{ dbt.type_string() }}) as source_order_description
  , cast(labloinc_comp.code as {{ dbt.type_string() }}) as source_component_code
  , cast('loinc' as {{ dbt.type_string() }}) as source_component_type
  , cast(labattributename.itemName as {{ dbt.type_string() }}) as source_component_description
  , cast(null as {{ dbt.type_string() }}) as normalized_order_type
  , cast(null as {{ dbt.type_string() }}) as normalized_order_code
  , cast(null as {{ dbt.type_string() }}) as normalized_order_description
  , cast(null as {{ dbt.type_string() }}) as normalized_component_code
  , cast(null as {{ dbt.type_string() }}) as normalized_component_type
  , cast(null as {{ dbt.type_string() }}) as normalized_component_description
  , cast(labdata.status as {{ dbt.type_string() }}) as status
  , cast(coalesce(labdatadetail.Value, labdata.result) as {{ dbt.type_string() }}) as result
  , cast(cast(labdata.ResultDate as datetime) + cast(labdata.resultime as time) as {{ dbt.type_timestamp() }}) as result_datetime
  , cast(cast(labdata.collDate as datetime) + cast(labdata.collTime as time) as {{ dbt.type_timestamp() }}) as collection_datetime
  , cast(null as {{ dbt.type_string() }}) as source_units
  , cast(null as {{ dbt.type_string() }}) as normalized_units
  , cast(null as {{ dbt.type_string() }}) as source_reference_range_low
  , cast(null as {{ dbt.type_string() }}) as source_reference_range_high
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
  {{ ref('stg_ecw__labdata') }} labdata
  inner join {{ ref('stg_ecw__enc') }} enc
    on enc.encounterid = labdata.encounterid
  left join {{ ref('stg_ecw__items') }} items
    on items.itemid = labdata.itemid
  left join {{ ref('stg_ecw__labdatadetail') }} labdatadetail
    on labdatadetail.reportid = labdata.reportid
  left join {{ ref('stg_ecw__items') }} labattributename
    on labattributename.itemid = labdatadetail.PropId
  left join {{ ref('stg_ecw__labcptmap') }} labcptmap
    on labcptmap.labCode = labdata.ItemId
  left join {{ ref('stg_ecw__labloinccodes') }} labloinc_order
    on labloinc_order.itemid = labdata.ItemId
  left join {{ ref('stg_ecw__labloinccodes') }} labloinc_comp
    on labloinc_comp.itemid = labdatadetail.PropId
where
  (labdata.result != '' OR labdatadetail.Value IS NOT NULL)