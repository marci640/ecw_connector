select
    Code
  , ShortDesc
  , LongDesc
  , case
      when codetype = '10' then 'icd-10-cm'
      when codetype = '9' then 'icd-9-cm'
      else null
    end as source_code_type_2
from
    {{ source('ecw','icd') }}