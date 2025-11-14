select
    encounterID
  , vitalID
  , propID
  , value
  , Id
  , UpdatedBy
  , UpdatedTime
from
    {{ source('ecw','vitals') }}