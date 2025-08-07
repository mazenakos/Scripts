select *
from test_result_new trn 
join parameter_outcomes_new pon 
on trn.test_id = pon.test_id
where trn.tester_name = 'Inverter-EOL-002'
and trn.test_start_time > '2024-07-01'
and pon.task_name in ('inv_pull_burn_in', 'inv_push_burn_in')
and pon.comparator = 'LOG'
and pon.type = 'Float'
and pon.parameter_name like '%_TemperaturePowerBoard%'
order by pon.param_value_float desc