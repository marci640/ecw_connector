select
    ID
  , PatientId
  , EncounterId
  , ScriptDate
  , ndccode
  , DrugName
  , Route
  , Quantity
  , Duration
  , doctorId
from
    {{ source('ecw','rxhub_scriptlog') }}