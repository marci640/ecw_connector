select 
    patientId
    , ImmunizationId
    , vaccinename
    , lotNumber
    , immstatus
    , Dose
    , DoseNumber
    , Route
    , givenDate
    , givenBy
    , location
    , cvx_code
    , vfc_code
    , encounterid
    , deleteFlag
from
    {{ source('ecw','immunizations') }}