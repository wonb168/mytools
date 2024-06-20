 select tenant_ssmd_adm.p_adm_batch_parameter_individual(423,
            (select array_agg(distinct a.parameter_code) parameter_codes
            from tenant_ssmd_biz.base_parameter_define a
            inner join
                (select * from tenant_ssmd_biz.base_parameter_define c
                where c."source" = 'PM'
                and c.applied_type in ('template','complex')
                and c.contain_extend_dim =0
                and c.is_deleted = 0
                ) b on a.parameter_code = b.parameter_code
            where a.parameter_type = 'LEVEL_TABLE'),
        cast('2024-05-13 00:00:00' as TIMESTAMP));