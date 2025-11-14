select
    Id
   , name
   , PracticeType
   , NPI
   , BillingAddressLine1
   , BillingAddressLine2
   , BillingCity
   , BillingState
   , BillingZip
   , PayableTo
   , AddressLine1
   , AddressLine2
   , City
   , State
   , Zip
from {{ source('ecw','edi_facilities') }}