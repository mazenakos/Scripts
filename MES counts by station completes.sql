
select substring(ph.station, 1, 3), DATE(ph.complete_time) as date, ph.process_key, count(distinct serial_number)
from mes.parts p 
join mes.process_histories ph on p.id = ph.part_id
where (ph.station like '%BMU%' or ph.station like '%SDS%' or ph.station like '%INV%')
and ph.complete_time > '2025-04-21'
and (ph.process_key like '%assemble_blower%' or  ph.process_key  like '%eol%' or ph.process_key like '%fasten_blower%' or ph.process_key like '%install_ground_bar%' or ph.process_key like '%hipot_test%' or ph.process_key like '%install_penguin' or ph.process_key like '%calibration%')
and ph.status in ('COMPLETED', 'OVERRIDDEN')
group by 1, 2, 3
order by 1, 2 asc