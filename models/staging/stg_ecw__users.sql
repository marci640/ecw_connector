select
    dob,
    sex,
    uemail,
    ufname,
    uid,
    ulname,
    concat(coalesce(ulname, ''), 
        ', 'coalesce(ufname, '')) 
        as uname,
    umobileno,
    upaddress,
    upaddress2,
    upcity,
    upPhone,
    upstate,
    UserType,
    zipcode
from
    {{ source('ecw','users') }}