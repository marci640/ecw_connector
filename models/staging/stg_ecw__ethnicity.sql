select *
from {{ source('ecw','ethnicity') }};