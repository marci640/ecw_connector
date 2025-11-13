select 
    itemid
  , code
from
    {{ source('ecw','labloinccodes') }}	