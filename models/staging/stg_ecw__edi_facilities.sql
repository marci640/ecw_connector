select
    Id,
    name as facility_name
from {{ source('ecw','edi_facilities') }}