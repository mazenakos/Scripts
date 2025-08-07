select cast(ph.process_sequence_number as int) as sequence, p.part_number, p.serial_number, ph.station, ph.process_key, ph.process_sequence_number, ph.status, ph.complete_time
from mes.parts p 
join mes.process_histories ph on p.id = ph.part_id
where p.serial_number = 'CE243200867'
order by 1 asc