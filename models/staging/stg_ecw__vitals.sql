select
    encounterID
  , vitalID
  , propID
  , value
  , Id
  , UpdatedBy
  , UpdatedTime
  , defaultValue
from
    {{ source('ecw','vitals') }}