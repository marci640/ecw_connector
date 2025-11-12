select
    ID, 
    PatientId,
    EncounterId,
    ScriptDate
    ndccode,
    DrugName,
    Route,
    TRY_CAST(Quantity AS {{ dbt.type_int() }}) AS Quantity,
    TRY_CAST(Duration AS {{ dbt.type_int() }}) AS Duration,
    doctorId
from
    {{ source('ecw','rxhub_scriptlog') }}
   
where
    Pt_LName not like 'Test%'