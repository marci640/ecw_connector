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
  , enc.startTime
  , enc.endTime
  , enc.arrivedTime
  , enc.depTime
  , encLock
from
    {{ source('ecw', 'enc') }}