select
    itemId
  , type
  , Id
  , mandatory
  , displayUom
from
    {{ source('ecw','vitaltypes') }}