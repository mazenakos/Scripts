select *
from test_result_new trn 
join parameter_outcomes_new pon 
on trn.test_id = pon.test_id
where trn.tester_name = 'Inverter-EOL-002'
and trn.part_num = 'APRD0005108'
and trn.test_start_time between '2025-02-01' and '2025-04-18'
and pon.task_name = 'inv_pull_burn_in'
and pon.comparator = 'LOG'
and pon.type = 'Float'
and pon.parameter_name like '%temp%'

