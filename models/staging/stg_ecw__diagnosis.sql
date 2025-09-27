select
    EncounterId,
    ItemId,
	displayIndex,
	PrimaryAsmt
    from
    {{ source('ecw','diagnosis') }}