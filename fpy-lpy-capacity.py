import datetime
import clickhouse_connect
import pandas as pd
from tabulate import tabulate

# -------- Prompt for valid date input --------
while True:
    user_input = input("Enter the start date (YYYY-MM-DD): ").strip()
    try:
        start_date = datetime.datetime.strptime(user_input, "%Y-%m-%d").date()
        break
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")

# -------- Set up ClickHouse client --------
try:
    client = clickhouse_connect.get_client(
        host='sxwqchyptv.us-west-2.aws.clickhouse.cloud',
        port=8443,
        username='mts_read_user',
        password='MTSreadUs3r!',
        secure=True
    )
except Exception as e:
    print(f"Connection failed: {e}")
    exit(1)

# -------- SQL Query --------
query = f"""
WITH toDate('{start_date}') AS start_date
SELECT 
    k.tester_name,
    k.location,
    k.distinct_serial_count,
    ROUND(k.first_pass_count / k.distinct_serial_count, 2) AS fpy,
    ROUND(k.last_pass_count / k.distinct_serial_count, 2) AS lpy,
    ROUND(d.avg_ct_min, 1) AS avg_ct_min,
    ROUND((20 * 8 * 60 * 0.75 * fpy / avg_ct_min), 0) as avail_capacity
FROM
(
    SELECT 
        COALESCE(f.tester_name, l.tester_name) AS tester_name,
        COALESCE(f.location, l.location) AS location,
        f.distinct_serial_count,
        f.first_pass_count,
        l.last_pass_count
    FROM
    (
        SELECT 
            tester_name, location,
            COUNT(DISTINCT serial_num) AS distinct_serial_count,
            SUM(CASE WHEN first_pass_result = 'pass' THEN 1 ELSE 0 END) AS first_pass_count
        FROM
        (
            SELECT 
                tester_name, location,
                serial_num, 
                argMin(result, test_end_time) AS first_pass_result
            FROM test_result_new
            WHERE test_end_time > start_date
            GROUP BY tester_name, location, serial_num
        ) AS sub
        GROUP BY tester_name, location
    ) f
    INNER JOIN
    (
        SELECT 
            tester_name, location,
            COUNT(DISTINCT serial_num) AS distinct_serial_count,
            SUM(CASE WHEN last_pass_result = 'pass' THEN 1 ELSE 0 END) AS last_pass_count
        FROM
        (
            SELECT 
                tester_name, location,
                serial_num, 
                argMax(result, test_end_time) AS last_pass_result
            FROM test_result_new
            WHERE test_end_time > start_date
            GROUP BY tester_name, location, serial_num
        ) AS sub
        GROUP BY tester_name, location
    ) l
    ON f.tester_name = l.tester_name AND f.location = l.location
) k
LEFT JOIN
(
    SELECT 
        tester_name, 
        AVG(duration_in_sec) / 60 AS avg_ct_min
    FROM test_result_new
    WHERE test_end_time > start_date
      AND result = 'pass'
    GROUP BY tester_name
) d
ON k.tester_name = d.tester_name
WHERE k.location NOT LIKE '%Reno%'
  AND k.location NOT LIKE '%RENO%'
  AND k.location NOT LIKE '%Richardson%'
  AND k.location NOT LIKE '%CRT%'
ORDER BY k.tester_name
"""

# -------- Run Query --------
try:
    print("\nRunning FPY/LPY query... this may take a moment.\n")
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)

    if df.empty:
        print("No data found for the given date.")
    else:
        print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))
except Exception as e:
    print(f"Query failed: {e}")
