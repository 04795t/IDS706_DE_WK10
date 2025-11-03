from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import csv
import os

DATA_FOLDER = "/opt/airflow/data"

# DAG settings
default_args = {"owner": "student", "retries": 1, "retry_delay": timedelta(minutes=1)}

dag = DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="Simple e-commerce pipeline with PySpark analysis",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["student", "assignment", "pyspark"],
)


# 1. Get customer data
@task(dag=dag)
def get_customers():
    """
    Load customer data
    """
    from faker import Faker

    fake = Faker()

    customers = []
    for i in range(1, 201):
        customers.append(
            {
                "customer_id": i,
                "name": fake.name(),
                "email": fake.email(),
                "city": fake.city(),
                "state": fake.state(),
                "signup_date": fake.date_between(start_date="-2y", end_date="-1y"),
            }
        )

    file_path = f"{DATA_FOLDER}/customers.csv"
    with open(file_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)

    print(f"Created {len(customers)} customers")
    return file_path


# 2. order data
@task(dag=dag)
def get_orders():
    """
    Load order data
    """
    from faker import Faker

    fake = Faker()

    products = ["Laptop", "Phone", "Tablet", "Headphones", "Monitor"]
    orders = []

    for i in range(1, 501):
        orders.append(
            {
                "order_id": i,
                "customer_id": fake.random_int(min=1, max=200),
                "product": fake.random_element(products),
                "price": round(fake.random.uniform(100, 1500), 2),
                "quantity": fake.random_int(min=1, max=3),
                "order_date": fake.date_between(start_date="-1y", end_date="today"),
            }
        )

    file_path = f"{DATA_FOLDER}/orders.csv"
    with open(file_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=orders[0].keys())
        writer.writeheader()
        writer.writerows(orders)

    print(f"Created {len(orders)} orders")
    return file_path


# 3. Clean customer data
@task(dag=dag)
def clean_customers(customer_file):
    """
    Add some calculations to customer data
    """
    df = pd.read_csv(customer_file)

    df["signup_date"] = pd.to_datetime(df["signup_date"])
    df["days_as_customer"] = (pd.Timestamp.now() - df["signup_date"]).dt.days

    df["customer_type"] = df["days_as_customer"].apply(
        lambda x: "New" if x < 365 else "Returning"
    )

    output = f"{DATA_FOLDER}/customers_clean.csv"
    df.to_csv(output, index=False)
    print(f"Cleaned customer data saved")
    return output


# 4. Clean order data
@task(dag=dag)
def clean_orders(order_file):
    """
    Add calculations to order data
    """
    df = pd.read_csv(order_file)

    df["total_price"] = df["price"] * df["quantity"]
    df["discount"] = df["quantity"].apply(lambda x: 0.10 if x >= 3 else 0)
    df["final_price"] = df["total_price"] * (1 - df["discount"])

    df["order_date"] = pd.to_datetime(df["order_date"])
    df["month"] = df["order_date"].dt.month

    output = f"{DATA_FOLDER}/orders_clean.csv"
    df.to_csv(output, index=False)
    print(f"Cleaned order data saved")
    return output


# 5. Combine the data
@task(dag=dag)
def combine_data(customer_file, order_file):
    """
    Merge customers and orders together
    """
    customers = pd.read_csv(customer_file)
    orders = pd.read_csv(order_file)

    combined = orders.merge(customers, on="customer_id", how="left")

    output = f"{DATA_FOLDER}/combined.csv"
    combined.to_csv(output, index=False)
    print(f"Combined {len(combined)} rows")
    return output


# 6. Load into database
@task(dag=dag)
def load_to_database(file_path):
    """
    Put data into PostgreSQL
    """
    df = pd.read_csv(file_path)

    hook = PostgresHook(postgres_conn_id="Postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA IF NOT EXISTS sales;")
    cursor.execute("DROP TABLE IF EXISTS sales.orders;")

    create_table = """
        CREATE TABLE sales.orders (
            order_id INTEGER,
            customer_id INTEGER,
            product TEXT,
            price NUMERIC,
            quantity INTEGER,
            order_date DATE,
            total_price NUMERIC,
            discount NUMERIC,
            final_price NUMERIC,
            month INTEGER,
            name TEXT,
            email TEXT,
            city TEXT,
            state TEXT,
            signup_date DATE,
            days_as_customer INTEGER,
            customer_type TEXT
        );
    """
    cursor.execute(create_table)

    for index, row in df.iterrows():
        insert = f"INSERT INTO sales.orders VALUES ({','.join(['%s']*len(row))});"
        cursor.execute(insert, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(df)} rows into database")
    return len(df)


# 7. Analyze with PySpark
@task(dag=dag)  # ← THIS WAS MISSING!
def create_analysis_with_spark():
    """
    Use PySpark to analyze sales data
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum as spark_sum
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    print("Starting PySpark for analysis...")

    spark = (
        SparkSession.builder.appName("SalesAnalysis").master("local[*]").getOrCreate()
    )

    print("PySpark session created!")

    hook = PostgresHook(postgres_conn_id="Postgres")
    query = "SELECT * FROM sales.orders;"
    df_pandas = hook.get_pandas_df(query)

    df_spark = spark.createDataFrame(df_pandas)

    print(f"Loaded {df_spark.count()} rows into Spark DataFrame")

    state_sales_spark = (
        df_spark.groupBy("state")
        .agg(spark_sum("final_price").alias("total_sales"))
        .orderBy(col("total_sales").desc())
        .limit(10)
    )

    print("PySpark aggregation complete!")

    state_sales = state_sales_spark.toPandas()

    spark.stop()
    print("PySpark session stopped")

    plt.figure(figsize=(10, 6))
    plt.bar(state_sales["state"], state_sales["total_sales"], color="steelblue")
    plt.title("Top 10 States by Sales (Analyzed with PySpark)", fontsize=14)
    plt.xlabel("State")
    plt.ylabel("Total Sales ($)")
    plt.xticks(rotation=45)
    plt.tight_layout()

    output = f"{DATA_FOLDER}/sales_chart.png"
    plt.savefig(output)
    plt.close()

    print(f"Chart created using PySpark analysis and saved to {output}")
    return output


# 8. Clean up files
@task(dag=dag)
def cleanup():
    """
    Delete temporary files
    """
    files = [
        "customers.csv",
        "orders.csv",
        "customers_clean.csv",
        "orders_clean.csv",
        "combined.csv",
    ]

    count = 0
    for file in files:
        path = f"{DATA_FOLDER}/{file}"
        if os.path.exists(path):
            os.remove(path)
            count += 1

    print(f"Cleaned up {count} files")


# workflow
customer_data = get_customers()
order_data = get_orders()

clean_customer = clean_customers(customer_data)
clean_order = clean_orders(order_data)

combined = combine_data(clean_customer, clean_order)
loaded = load_to_database(combined)
chart = create_analysis_with_spark()
clean = cleanup()

# order of operations
loaded >> chart >> clean
