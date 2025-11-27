select
    encounterID
  , patientID
  , invoiceID
  , facilityId
  , ResourceId
  , doctorID
  , ClaimReq
  , date
  , encType
  , STATUS
  , reason
  , VisitType
  , deleteFlag
  , startTime
  , endTime
  , arrivedTime
  , depTime
  , encLock
from
    {{ source('ecw', 'enc') }}