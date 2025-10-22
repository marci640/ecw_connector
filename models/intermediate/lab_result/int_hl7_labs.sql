SELECT
    CAST(labdata.ReportId AS VARCHAR) + '-' + CAST(hl7.hl7itemid AS VARCHAR) AS lab_result_id,
    enc.patientID AS person_id,
    enc.patientID AS patient_id,
    labdata.EncounterId AS encounter_id,
    labdata.ReportId AS accession_number,
    CASE WHEN labcptmap.cptcode IS NOT NULL THEN 'hcpcs' ELSE 'loinc' END AS source_order_type,
    COALESCE(labcptmap.cptcode, labloinc_order_hl7.code) AS source_order_code,
    items.itemName AS source_order_description,
    'loinc' AS source_component_type,
    labloinc_comp_hl7.code AS source_component_code,
    hl7.name AS source_component_description,
    labdata.status AS status,
    COALESCE(hl7.value, labdata.result) AS result,
    CAST(labdata.ResultDate AS DATETIME) + CAST(labdata.resultime AS TIME) AS result_datetime,
    CAST(labdata.collDate AS DATETIME) + CAST(labdata.collTime AS TIME) AS collection_datetime,
    hl7.units AS source_units,
    SUBSTRING(hl7.range, 1, CHARINDEX('-', hl7.range) - 1) AS source_reference_range_low,
    SUBSTRING(hl7.range, CHARINDEX('-', hl7.range) + 1, LEN(hl7.range)) AS source_reference_range_high,
    NULL AS source_abnormal_flag,
    NULL AS specimen,
    labdata.ordPhyId AS ordering_practitioner_id,
    'eCW' AS data_source,
    'hl7_labs' AS _src
FROM
    {{ ref('stg_ecw__hl7labdatadetail') }} hl7
    LEFT JOIN {{ ref('stg_ecw__labdata') }} labdata
        ON labdata.ReportId = hl7.ReportId
    LEFT JOIN {{ ref('stg_ecw__enc') }} enc
        ON enc.encounterID = labdata.EncounterId
    LEFT JOIN {{ ref('stg_ecw__items') }} items
        ON items.itemid = labdata.itemid
    LEFT JOIN {{ ref('stg_ecw__labcptmap') }} labcptmap
        ON labcptmap.labCode = labdata.ItemId
    LEFT JOIN {{ ref('stg_ecw__labloinccodes') }} labloinc_order_hl7
        ON labloinc_order_hl7.itemid = labdata.ItemId
    LEFT JOIN {{ ref('stg_ecw__labloinccodes') }} labloinc_comp_hl7
        ON labloinc_comp_hl7.itemid = hl7.hl7itemid
WHERE
    hl7.value != 'TNP';