SELECT 
    a1.tester_name,
    a1.date,
    a1.distinct_pass_count,
    a2.distinct_fail_count
FROM (
    SELECT 
        trn.tester_name, 
        DATE(trn.test_end_time) AS date, 
        COUNT(DISTINCT trn.serial_num) AS distinct_pass_count
    FROM test_result_new trn 
    WHERE trn.tester_name IN (
        'ALC-PCBA-003', 'ALP-PCBA-001', 'BIPBIC-PCBA-003', 'BIPBIC-PCBA-004',
        'BMU-EOL-001', 'BMU-HIPOT-001', 'CAB-SWITCH-001', 'INV-HIPOT-001',
        'Inverter-EOL-002', 'Manta-PCBA-002', 'PGN-FLASH-001', 'PGN-FLASH-002',
        'SDS-CAL-001', 'SDS-EOL-001', 'SDS-HIPOT-001'
    )
    AND trn.test_end_time > '2025-06-10'
    AND trn.result = 'pass'
    AND trn.serial_num NOT IN ('RE250600002', 'RE250800006', 'RE250600007')
    GROUP BY trn.tester_name, DATE(trn.test_end_time)
) a1
JOIN (
    SELECT 
        trn.tester_name, 
        DATE(trn.test_end_time) AS date, 
        COUNT(DISTINCT trn.serial_num) AS distinct_fail_count
    FROM test_result_new trn 
    WHERE trn.tester_name IN (
        'ALC-PCBA-003', 'ALP-PCBA-001', 'BIPBIC-PCBA-003', 'BIPBIC-PCBA-004',
        'BMU-EOL-001', 'BMU-HIPOT-001', 'CAB-SWITCH-001', 'INV-HIPOT-001',
        'Inverter-EOL-002', 'Manta-PCBA-002', 'PGN-FLASH-001', 'PGN-FLASH-002',
        'SDS-CAL-001', 'SDS-EOL-001', 'SDS-HIPOT-001'
    )
    AND trn.test_end_time > '2025-06-10'
    AND trn.result = 'fail'
    AND trn.serial_num NOT IN ('RE250600002', 'RE250800006', 'RE250600007')
    GROUP BY trn.tester_name, DATE(trn.test_end_time)
) a2 ON a1.tester_name = a2.tester_name AND a1.date = a2.date
ORDER BY a1.tester_name DESC, a1.date ASC;