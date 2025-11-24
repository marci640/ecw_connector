select
    pid
  , doctorId
  , pmcId
  , maritalstatus
  , deceased
  , deceasedDate
  , language
  , GrId
  , hl7Id
  , race
  , PtStatus
  , ethnicity
from {{ source('ecw','patients') }};