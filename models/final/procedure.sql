{{ dbt_utils.union_relations(
    relations=[ ref('int_enc_procedure'),
                ref('int_lab_procedure') ]
) }}