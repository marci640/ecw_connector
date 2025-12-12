select
      cast(concat(enc.patientId, '-', enc.date, '-', itemdetail.value) as {{ dbt.type_string() }}) as procedure_id
    , cast(enc.patientId as {{ dbt.type_string() }}) as person_id
    , cast(enc.patientId as {{ dbt.type_string() }}) as patient_id
    , cast(enc.encounterId as {{ dbt.type_string() }}) as encounter_id
    , cast(enc.invoiceId as {{ dbt.type_string() }}) as claim_id
    , cast(enc.date as date) as procedure_date
    , cast('hcpcs' as {{ dbt.type_string() }}) as source_code_type
    , cast(itemdetail.value as {{ dbt.type_string() }}) as source_code
    , cast(items.itemName as {{ dbt.type_string() }}) as source_description
    , cast(null as {{ dbt.type_string() }}) as normalized_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_code
    , cast(null as {{ dbt.type_string() }}) as normalized_description
	, cast(billingdata.Mod1 as {{ dbt.type_string() }}) as modifier_1
	, cast(billingdata.Mod2 as {{ dbt.type_string() }}) as modifier_2
	, cast(billingdata.Mod3 as {{ dbt.type_string() }}) as modifier_3
	, cast(billingdata.Mod4 as {{ dbt.type_string() }}) as modifier_4
	, cast(null as {{ dbt.type_string() }}) as modifier_5
	, cast(enc.ResourceId as {{ dbt.type_string() }}) as practitioner_id
	, cast('ecw' as {{ dbt.type_string() }}) as data_source
	, cast(null as {{ dbt.type_string() }}) as file_name
	, cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime

from
    {{ ref('stg_ecw__billingdata') }} billingdata
    inner join {{ ref('stg_ecw__enc') }} enc
      on enc.encounterID = billingdata.EncounterId
    inner join {{ ref('stg_ecw__itemdetail') }} itemdetail
      on itemdetail.itemID = billingdata.ItemID
	inner join {{ ref('stg_ecw__items') }} items
	  on items.ItemID = billingdata.itemID
where
    itemdetail.propid = 13