select
    Id
   , name as facility_name
   , NPI
   , BillingAddressLine1
   , BillingAddressLine2
   , BillingCity
   , BillingState
   , BillingZip
from {{ source('ecw','edi_facilities') }}