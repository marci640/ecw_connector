SELECT
    CAST(labdata.ReportId AS VARCHAR) + '-' + CAST(labdatadetail.PropId AS VARCHAR) AS lab_result_id,
    enc.patientID AS person_id,
    enc.patientID AS patient_id,
    labdata.EncounterId AS encounter_id,
    labdata.ReportId AS accession_number,
    CASE WHEN labcptmap.cptcode IS NOT NULL THEN 'hcpcs' ELSE 'loinc' END AS source_order_type,
    COALESCE(labcptmap.cptcode, labloinc_order.code) AS source_order_code,
    items.itemName AS source_order_description,
    'loinc' AS source_component_type,
    labloinc_comp.code AS source_component_code,
    labattributename.itemName AS source_component_description,
    labdata.status AS status,
    COALESCE(labdatadetail.Value, labdata.result) AS result,
    CAST(labdata.ResultDate AS DATETIME) + CAST(labdata.resultime AS TIME) AS result_datetime,
    CAST(labdata.collDate AS DATETIME) + CAST(labdata.collTime AS TIME) AS collection_datetime,
    NULL AS source_units,
    NULL AS source_reference_range_low,
    NULL AS source_reference_range_high,
    NULL AS source_abnormal_flag,
    NULL AS specimen,
    labdata.ordPhyId AS ordering_practitioner_id,
    'eCW' AS data_source,
    'standard_labs' AS _src
    
FROM
    {{ ref('stg_ecw__labdata') }} labdata
    INNER JOIN {{ ref('stg_ecw__enc') }} enc
        ON enc.encounterid = labdata.encounterid
    LEFT JOIN {{ ref('stg_ecw__items') }} items
        ON items.itemid = labdata.itemid
    LEFT JOIN {{ ref('stg_ecw__labdatadetail') }} labdatadetail
        ON labdatadetail.reportid = labdata.reportid
    LEFT JOIN {{ ref('stg_ecw__items') }} labattributename
        ON labattributename.itemid = labdatadetail.PropId
    LEFT JOIN {{ ref('stg_ecw__labcptmap') }} labcptmap
        ON labcptmap.labCode = labdata.ItemId
    LEFT JOIN {{ ref('stg_ecw__labloinccodes') }} labloinc_order
        ON labloinc_order.itemid = labdata.ItemId
    LEFT JOIN {{ ref('stg_ecw__labloinccodes') }} labloinc_comp
        ON labloinc_comp.itemid = labdatadetail.PropId
WHERE
    (labdata.result != '' OR labdatadetail.Value IS NOT NULL)