select
    EncounterId
  , ReportId
  , ResultDate
  , result
  , received
  , status
  , ReviewedDate
  , priority
  , ItemId
  , assignedToId
  , ReviewedBy
from
    {{ source('ecw','labdata') }}
where
    deleteFlag = 0
  and cancelled = 0