select 
	EncounterId,
	ReportId,
	CONVERT(varchar(10), cast(ResultDate as date), 101) AS ResultDate,
	result,
	received,
	status,
	CONVERT(varchar(10), cast(ReviewedDate as date), 101) AS ReviewedDate,
	priority,
    ItemId,
    assignedToId,
    ReviewedBy
from
    {{ source('ecw','labdata') }}
where 
	deleteFlag = 0
	and cancelled = 0	