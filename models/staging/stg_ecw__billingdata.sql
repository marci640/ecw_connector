select
    Id
  , EncounterId
  , icdcode1
  , icdcode2
  , icdcode3
  , icdcode4
  , itemID
  , Mod1
  , Mod2
  , Mod3
  , Mod4
  , displayIndex
  , deleteFlag
from {{ source('ecw', 'billingdata') }}