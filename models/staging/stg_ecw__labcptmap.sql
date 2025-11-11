select 
    labcode,
    cptcode
from (
    select
        labcode,
        cptcode,
        row_number() over (partition by labcode order by cptcode) as rn
    from
        {{ source('ecw','labcptmap') }}
) t
where rn = 1

-- One lab order can map to multiple CPT codes; pick the first