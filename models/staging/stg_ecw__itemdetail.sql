select
    itemID
  , value
  , propID
from
    {{ source('ecw','itemdetail') }}