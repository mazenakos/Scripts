from clickhouse_driver import Client
import pandas as pd

# Connection config - fill in from your JDBC URL
CLICKHOUSE_HOST = 'sxwqchyptv.us-west-2.aws.clickhouse.cloud'
CLICKHOUSE_PORT = 8443
CLICKHOUSE_USER = 'mts_read_user'
CLICKHOUSE_PASSWORD = 'MTSreadUs3r!'

def main():
    # Create client with SSL enabled
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        secure=True
    )

    # Step 1: Simple test query to verify connection
    print("Testing connection...")
    try:
        test_result = client.execute("SELECT now()")
        print("Server time:", test_result[0][0])
    except Exception as e:
        print("Connection test failed:", e)
        return

    # Step 2: Prompt for date input
    start_date = input("Enter start date (YYYY-MM-DD): ")

    # Step 3: Your full query
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
    WHERE k.location NOT ILIKE '%Reno%'
      AND k.location NOT ILIKE '%Richardson%'
      AND k.location NOT ILIKE '%CRT%'
    ORDER BY k.tester_name
    """

    print("\nRunning FPY/LPY query... this may take a moment.\n")
    try:
        rows = client.execute(query)
        # Fetch column names by describing query result structure
        columns_query = f"DESCRIBE TABLE test_result_new"
        # We'll hardcode columns as per SELECT because DESCRIBE TABLE on subquery is tricky
        columns = [
            'tester_name', 'location', 'distinct_serial_count', 
            'fpy', 'lpy', 'avg_ct_min', 'avail_capacity'
        ]
        df = pd.DataFrame(rows, columns=columns)
        print(df.to_string(index=False))
    except Exception as e:
        print("Query failed:", e)

if __name__ == "__main__":
    main()
