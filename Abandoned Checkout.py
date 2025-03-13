import os
import json
import pandas as pd
from google.cloud import bigquery
import matplotlib.pyplot as plt

def run_abandoned_checkout_analysis():
    """
    Analyzes abandonment patterns from the `truegloryhair.tgh1.abandoned_checkouts` table,
    focusing on product types, cart size, and timing (e.g., hour of day).
    """
# Set your project ID
    project_id = "truegloryhair"
    
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)

    query = """
    SELECT
        id AS abandoned_checkout_id,
        abandonedCheckoutUrl,
        createdAt,
        updatedAt,
        completedAt,
        customer_id,
        customer_firstName,
        customer_lastName,
        customer_email,
        lineItems_edges,
        subtotalPriceSet_shopMoney_amount AS subtotal_amount,
        totalPriceSet_shopMoney_amount AS total_amount
    FROM `truegloryhair.tgh1.abandoned_checkouts`
    """
    
    query_job = client.query(query)
    results = query_job.result()

    df = results.to_dataframe()
    
    # Convert date/time columns to pandas datetime
    for col in ["createdAt", "updatedAt", "completedAt"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

   
    def extract_product_titles(lineitems):
        if not lineitems:
            return []
        try:
            items_raw = lineitems.split(";")
        except AttributeError:
            items_raw = [lineitems]
        
        product_titles = []
        for item_raw in items_raw:
            item_raw = item_raw.strip()
            if item_raw:
                json_str = item_raw.replace("'", "\"")
                try:
                    item_dict = json.loads(json_str)
                    if "node" in item_dict and "title" in item_dict["node"]:
                        product_titles.append(item_dict["node"]["title"])
                except json.JSONDecodeError:
                    pass
        return product_titles

    df["product_titles"] = df["lineItems_edges"].apply(extract_product_titles)

  
    df_exploded = df.explode("product_titles")
    product_type_counts = df_exploded["product_titles"].value_counts().reset_index()
    product_type_counts.columns = ["product_title", "abandon_count"]
    
    print("=== Abandonment Count by Product Title ===")
    print(product_type_counts)
    print()

    # 5B. By Cart Size
    bin_edges = [0, 50, 100, 200, 300, 500, 1000, float("inf")]
    bin_labels = ["0-50", "50-100", "100-200", "200-300", "300-500", "500-1000", "1000+"]
    
    df["cart_size_bin"] = pd.cut(df["total_amount"], bins=bin_edges, labels=bin_labels, right=False)
    cart_size_counts = df["cart_size_bin"].value_counts().sort_index()
    
    print("=== Abandonment Count by Cart Size Range ===")
    print(cart_size_counts)
    print()

    # 5C. By Timing (Hour of Day)
    df["abandoned_hour"] = df["createdAt"].dt.hour
    hour_counts = df["abandoned_hour"].value_counts().sort_index()
    
    print("=== Abandonment Count by Hour of Day ===")
    print(hour_counts)
    print()

 
    plt.figure()
    product_type_counts.head(10).plot(
        x="product_title", 
        y="abandon_count", 
        kind="bar"
    )
    plt.title("Top 10 Abandoned Product Titles")
    plt.xlabel("Product Title")
    plt.ylabel("Abandonment Count")
    plt.tight_layout()
    # Save figure as PNG
    plt.savefig("product_titles.png")

    # Cart size bins
    plt.figure()
    cart_size_counts.plot(kind="bar")
    plt.title("Abandonment Count by Cart Size Range")
    plt.xlabel("Cart Size Range (USD)")
    plt.ylabel("Abandonment Count")
    plt.tight_layout()
    # Save figure as PNG
    plt.savefig("cart_size_range.png")

    # Hour of day
    plt.figure()
    hour_counts.plot(kind="bar")
    plt.title("Abandonment Count by Hour of Day")
    plt.xlabel("Hour of Day (0-23)")
    plt.ylabel("Abandonment Count")
    plt.tight_layout()
    # Save figure as PNG
    plt.savefig("hour_of_day.png")


if __name__ == "__main__":
    run_abandoned_checkout_analysis()
