select
    ItemId,
case
    when icd10flag = 1 then 'icd-10-cm'
    when icd10flag = 0 then 'icd-9-cm'
    else NULL
end  as  "source_code_type_1"

from
    {{ source('ecw','edi_icdcodes') }}