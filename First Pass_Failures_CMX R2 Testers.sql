SELECT
    tester_name,
    error_str,
    COUNT(*) AS failure_count
FROM
(
    SELECT
        tester_name,
        serial_num,
        error_str,
        test_end_time
    FROM
    (
        SELECT
            tester_name,
            serial_num,
            error_str,
            test_end_time,
            is_debug,
            row_number() OVER (PARTITION BY tester_name, serial_num ORDER BY test_end_time ASC) AS rn
        FROM test_result_new trn 
    )
    WHERE rn = 1
      AND error_str != ''  -- Only count failures (non-empty error)
      AND test_end_time > '2025-07-01'
      AND is_debug = 0
)
GROUP BY tester_name, error_str
ORDER BY tester_name, failure_count DESC;
