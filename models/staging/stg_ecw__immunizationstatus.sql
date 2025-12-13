select 
    id
    , ImmStatusId
    , CODE
    , Description

from
    {{ source('ecw','immunizationstatus') }}