select
    ReportId
  , hl7itemid
  , name
  , value
  , units
  , range
from
    {{ source('ecw','hl7labdatadetail') }}