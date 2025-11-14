select
    id
  , itemid
  , CODE
  , UpdatedBy
  , UpdatedTime
  , LoincType
  , deleteflag
from
    {{ source('ecw','loinccodes') }}