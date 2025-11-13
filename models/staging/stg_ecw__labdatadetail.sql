select
    reportid
  , propId
  , Value
from
    {{ source('ecw','labdatadetail') }}