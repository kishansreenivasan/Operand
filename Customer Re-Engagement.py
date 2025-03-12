from google.cloud import bigquery

def run_rfm_query():
    # Set your project ID
    project_id = "truegloryhair"
    
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)

    # Define the query
    query = """
    WITH customer_orders AS (
        SELECT
            email,
            DATE(processedAt) AS order_date,
            CAST(currentTotalPriceSet_shopMoney_amount AS FLOAT64) AS order_amount
        FROM `truegloryhair.tgh1.orders`
        WHERE email IS NOT NULL
    ),
    customer_metrics AS (
        SELECT
            email,
            DATE_DIFF(CURRENT_DATE(), MAX(order_date), DAY) AS recency,
            COUNT(*) AS frequency,
            SUM(order_amount) AS monetary,
            SAFE_DIVIDE(SUM(order_amount), COUNT(*)) AS avg_order_value
        FROM customer_orders
        GROUP BY email
    ),
    top_10_thresholds AS (
        SELECT
            PERCENTILE_CONT(monetary, 0.90) OVER () AS monetary_90,
            PERCENTILE_CONT(frequency, 0.90) OVER () AS frequency_90,
            PERCENTILE_CONT(avg_order_value, 0.90) OVER () AS aov_90
        FROM customer_metrics
    )
    
    SELECT DISTINCT
        cm.email,
        cm.recency,
        cm.frequency,
        cm.monetary,
        cm.avg_order_value
    FROM customer_metrics cm
    CROSS JOIN top_10_thresholds
    WHERE 
        cm.recency > 60  -- Customers who haven't engaged in the last 60 days
        AND (cm.monetary >= monetary_90 OR cm.frequency >= frequency_90 OR cm.avg_order_value >= aov_90)
    ORDER BY cm.monetary DESC, cm.frequency DESC, cm.avg_order_value DESC;
    """

    # Execute the query
    query_job = client.query(query)
    results = query_job.result()  # Wait for the job to complete.

    # Store unique results in a set to avoid duplicate entries
    unique_results = set()

    # Print the results
    print("email, recency, frequency, monetary, avg_order_value")
    for row in results:
        entry = (row.email, row.recency, row.frequency, row.monetary, row.avg_order_value)
        if entry not in unique_results:
            unique_results.add(entry)
            print(f"{row.email}, {row.recency}, {row.frequency}, {row.monetary}, {row.avg_order_value}")

if __name__ == "__main__":
    run_rfm_query()

