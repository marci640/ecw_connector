select
    Id
  , InvoiceId
  , ItemId
  , Code
  , PrimaryCode
  , Mod1
  , Mod2
  , Mod3
  , Mod4
  , POS
  , BilledFee
  , NonCoveredCharges
  , ICD1
  , ICD2
  , ICD3
  , ICD4
  , AllowedFee
  , PtPortion
  , TotalPtPortion
  , OrdPrUserId
  , OrdPrNPI
  , RefPrUserId
  , RefPrNPI
  , FacilityId
  , RendPrUserId
  , RendPrNPI
  , SupPrUserId
  , SupPrNPI
  , Notes
  , BillToIns
  , BillToPt
  , RevCodeId
  , RevCode
  , ServiceCode
  , cptOrder
  , deleteFlag
  , displayIndex
  , BilledToIdType
  , BilledToId
  , BilledToIdSeqNo
  , FileStatus

from {{ source('ecw', 'edi_inv_cpt') }}