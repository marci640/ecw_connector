select 
    EthId
    , Code
    , Name

from {{ source('ecw','ethnicity') }}