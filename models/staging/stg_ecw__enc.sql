select
    encounterID,
    patientID,
    invoiceID,
    facilityId,
    ResourceId,
    doctorID,
    ClaimReq
    date,
    encType,
    STATUS,
    reason,
    VisitType,
    deleteFlag,
    convert(varchar(5), enc.startTime, 108) AS startTime, 
	convert(varchar(5), enc.endTime, 108) AS endTime, 
	convert(varchar(5), enc.arrivedTime, 108) AS arrivedTime,
	convert(varchar(5), enc.depTime, 108) AS depTime,
    encLock
from {{ source('ecw', 'enc')}}