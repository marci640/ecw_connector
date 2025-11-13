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
  , cancelled
  , convert(varchar(5), enc.startTime, 108) as startTime
  , convert(varchar(5), enc.endTime, 108) as endTime
  , convert(varchar(5), enc.arrivedTime, 108) as arrivedTime
  , convert(varchar(5), enc.depTime, 108) as depTime
  , encLock
from
    {{ source('ecw', 'enc') }}