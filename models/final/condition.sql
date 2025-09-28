{{ dbt_utils.union_relations(
    relations=[ ref('int_enc_diagnosis'),
                ref('int_problem_list') ]
) }}