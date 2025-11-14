select
    itemid
  , lowRange
  , highRange
  , groupId
  , invalidLow
  , invalidHigh
  , vitaltype
  , vital_low
  , vital_high
from
    {{ source('ecw','vitalrange') }}