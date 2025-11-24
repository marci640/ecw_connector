select
    dob
  , sex
  , uemail
  , ufname
  , uid
  , ulname
  , uminitial
  , uname
  , suffix
  , umobileno
  , upaddress
  , upaddress2
  , upcity
  , upPhone
  , upstate
  , county
  , UserType
  , zipcode
from
    {{ source('ecw','users') }}