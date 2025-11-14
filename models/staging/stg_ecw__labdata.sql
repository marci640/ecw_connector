select
    ReportId
  , EncounterId
  , ItemId
  , result
  , received
  , status
  , ResultDate
  , collDate
  , collTime
  , resultime
  , ReviewedDate
  , ReviewedBy
  , ordPhyId
  , priority
  , assignedToId
  , deleteFlag
  , cancelled
from
    {{ source('ecw','labdata') }}
where
    deleteFlag = 0
  and cancelled = 0