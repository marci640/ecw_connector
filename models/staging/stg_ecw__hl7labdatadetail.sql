select
    ReportId
  , name
  , value
  , units
  , range
from
    {{ source('ecw','hl7labdatadetail') }}
where
    deleteFlag = 0
  and cancelled = 0