select 
    itemId,
    keyName,
    itemName,
from
    {{ source('ecw','items') }}	