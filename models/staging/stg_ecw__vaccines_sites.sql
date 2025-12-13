select
    Code,
    Location 
from
    {{ source('ecw','vaccines_sites') }}