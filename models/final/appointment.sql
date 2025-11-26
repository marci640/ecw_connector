select
      cast(concat(enc.encounterID, '-', enc.STATUS) as {{ dbt.type_string() }}) as appointment_id
    , cast(enc.patientID as {{ dbt.type_string() }}) as person_id
    , cast(enc.patientID as {{ dbt.type_string() }}) as patient_id
    , cast(enc.encounterID as {{ dbt.type_string() }}) as encounter_id
    , cast(null as {{ dbt.type_string() }}) as source_appointment_type_code
    , cast(null as {{ dbt.type_string() }}) as source_appointment_type_description
    , cast(null as {{ dbt.type_string() }}) as normalized_appointment_type_code
    , cast(null as {{ dbt.type_string() }}) as normalized_appointment_type_description
    , cast(enc.startTime as {{ dbt.type_timestamp() }}) as start_datetime
    , cast(enc.endTime as {{ dbt.type_timestamp() }}) as end_datetime
    , datediff(minute,
        cast(enc.startTime as {{ dbt.type_timestamp() }}),
        cast(enc.endTime as {{ dbt.type_timestamp() }})
      ) as duration
    , cast(enc.facilityId as {{ dbt.type_string() }}) as location_id
    , cast(enc.resourceID as {{ dbt.type_string() }}) as practitioner_id
    , cast(
        case
            when enc.STATUS = 'R/S' then 'booked'
            when enc.STATUS = 'CANC' then 'cancelled'
            when enc.STATUS = 'CANCPHONE' then 'cancelled'
            when enc.STATUS = 'CANCCLINIC' then 'cancelled'
            when enc.STATUS = 'CANCSMS' then 'cancelled'
            when enc.STATUS = 'CANCSTAFF' then 'cancelled'
            when enc.STATUS = 'N/S' then 'noshow'
            when enc.STATUS = 'WO' then 'cancelled'
            when enc.STATUS = 'CHK' then 'fulfilled'
            when enc.STATUS = 'PEN' then 'booked'
            when enc.STATUS = 'CONFPHONE' then 'booked'
            when enc.STATUS = 'CONFSMS' then 'booked'
            when enc.STATUS = 'RSPHONE' then 'booked'
            when enc.STATUS = 'ANSPH' then 'booked'
            when enc.STATUS = 'VOICEMSG' then 'booked'
            when enc.STATUS = 'FAILEDMSG' then 'booked'
            when enc.STATUS = 'READY' then 'arrived'
            when enc.STATUS = 'ARR' then 'arrived'
            when enc.STATUS = 'QR_ARR' then 'arrived'
            else 'pending'
        end
        as {{ dbt.type_string() }}) as source_status
    , cast(null as {{ dbt.type_string() }}) as normalized_status
    , cast(doctors.speciality as {{ dbt.type_string() }}) as appointment_specialty
    , cast(enc.reason as {{ dbt.type_string() }}) as reason
    , cast(diagnosis.source_code_type as {{ dbt.type_string() }}) as source_reason_code_type
    , cast(diagnosis.source_code as {{ dbt.type_string() }}) as source_reason_code
    , cast(diagnosis.source_description as {{ dbt.type_string() }}) as source_reason_description
    , cast(null as {{ dbt.type_string() }}) as normalized_reason_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_reason_code
    , cast(null as {{ dbt.type_string() }}) as normalized_reason_description
    , cast(null as {{ dbt.type_string() }}) as cancellation_reason
    , cast(null as {{ dbt.type_string() }}) as source_cancellation_reason_code_type
    , cast(null as {{ dbt.type_string() }}) as source_cancellation_reason_code
    , cast(null as {{ dbt.type_string() }}) as source_cancellation_reason_description
    , cast(null as {{ dbt.type_string() }}) as normalized_cancellation_reason_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_cancellation_reason_code
    , cast(null as {{ dbt.type_string() }}) as normalized_cancellation_reason_description
    , cast('ecw' as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from
    {{ ref('stg_ecw__enc') }} enc
    inner join {{ ref('stg_ecw__doctors') }} doctors
        on enc.resourceID = doctors.doctorID
    left join {{ ref('int_enc_diagnosis') }} diagnosis
        on diagnosis.encounter_id = enc.encounterID and diagnosis.condition_rank = 1