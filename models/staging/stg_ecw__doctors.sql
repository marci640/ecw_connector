select
    doctorID,
    deaNo,
    FaxNo,
    speciality,
    SpecialityCode,
    PrintName,
    providerCode,
    LicenseKey,
    TaxID,
    TaxIDType,
    TaxIDSuffix,
    FacilityId,
    NPI
from {{ source('ecw', 'doctors') }};