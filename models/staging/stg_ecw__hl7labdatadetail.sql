select
    ReportId
  , name
  , value
  , units
  , range
from
    {{ source('ecw','hl7labdatadetail') }}