select
    patientId,
    encounterId,
    AddedDate,
    onsetdate,
    resolvedon,
    asmtId,
    case
        when inactiveFlag = 1 then 'inactive'
        when Resolved = 1 then 'resolved'
        else 'active'
    end as status,
from
    {{ source('ecw','problemlist') }}
where
    deleteFlag = 0