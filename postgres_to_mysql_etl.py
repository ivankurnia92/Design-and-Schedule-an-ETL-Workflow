from datetime import timedelta
import logging
import re

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago



# Default Arguments
default_args = {
    'owner': 'data-engineering-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}



# DAG Definition
dag = DAG(
    dag_id='postgres_to_mysql_etl',
    default_args=default_args,
    description='ETL pipeline from PostgreSQL to MySQL',
    schedule_interval=timedelta(hours=6),
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'postgresql', 'mysql', 'data-pipeline'],
)



# Extraction Functions

def extract_customers_from_postgres(**context):
    
    # Ekstrak data customer dari PostgreSQL
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = """
        SELECT *
        FROM raw_data.customers
        WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
    """

    try:
        logging.info("Extracting customers from PostgreSQL")
        df = hook.get_pandas_df(sql)
        data = df.to_dict(orient='records')

        context['ti'].xcom_push(key='customers_data', value=data)
        logging.info("Successfully extracted %s customers", len(data))
    except Exception as exc:
        logging.error("Failed to extract customers: %s", exc)
        raise


def extract_products_from_postgres(**context):
    
    # Ekstrak data product dari PostgreSQL
    hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = """
        SELECT
            p.*,
            s.supplier_name
        FROM raw_data.products p
        JOIN raw_data.suppliers s
            ON p.supplier_id = s.id
        WHERE p.updated_at >= CURRENT_DATE - INTERVAL '1 day'
    """

    try:
        logging.info("Extracting products from PostgreSQL")
        df = hook.get_pandas_df(sql)
        data = df.to_dict(orient='records')

        context['ti'].xcom_push(key='products_data', value=data)
        logging.info("Successfully extracted %s products", len(data))
    except Exception as exc:
        logging.error("Failed to extract products: %s", exc)
        raise


def extract_orders_from_postgres(**context):
    
    # Ekstrak data order dari PostgreSQL
    hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = """
        SELECT *
        FROM raw_data.orders
        WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
    """

    try:
        logging.info("Extracting orders from PostgreSQL")
        df = hook.get_pandas_df(sql)
        data = df.to_dict(orient='records')

        context['ti'].xcom_push(key='orders_data', value=data)
        logging.info("Successfully extracted %s orders", len(data))
    except Exception as exc:
        logging.error("Failed to extract orders: %s", exc)
        raise



# Transform & Load

def transform_and_load_customers(**context):
    
    # Transform data customer dan load ke MySQL
    
    customers = context['ti'].xcom_pull(
        task_ids='extract_customers',
        key='customers_data'
    )

    if not customers:
        logging.info("No customer data to process")
        return

    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    sql = """
        INSERT INTO dim_customers (
            id, name, email, phone, state, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            email = VALUES(email),
            phone = VALUES(phone),
            state = VALUES(state),
            updated_at = VALUES(updated_at)
    """

    try:
        for customer in customers:
            digits = re.sub(r'\D', '', str(customer.get('phone', '')))
            if len(digits) == 10:
                customer['phone'] = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"

            customer['state'] = str(customer.get('state', '')).upper()

            cursor.execute(
                sql,
                (
                    customer['id'],
                    customer['name'],
                    customer['email'],
                    customer['phone'],
                    customer['state'],
                    customer['updated_at'],
                )
            )

        conn.commit()
        logging.info("Customers successfully loaded to MySQL")
    except Exception as exc:
        conn.rollback()
        logging.error("Failed to load customers: %s", exc)
        raise
    finally:
        cursor.close()
        conn.close()


def transform_and_load_products(**context):
    
    # Transform data product dan load ke MySQL
    products = context['ti'].xcom_pull(
        task_ids='extract_products',
        key='products_data'
    )

    if not products:
        logging.info("No product data to process")
        return

    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    sql = """
        INSERT INTO dim_products (
            id, product_name, supplier_name,
            price, cost, margin, category
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            product_name = VALUES(product_name),
            supplier_name = VALUES(supplier_name),
            price = VALUES(price),
            cost = VALUES(cost),
            margin = VALUES(margin),
            category = VALUES(category)
    """

    try:
        for product in products:
            price = product.get('price', 0)
            cost = product.get('cost', 0)
            product['margin'] = ((price - cost) / price) * 100 if price > 0 else 0
            product['category'] = str(product.get('category', '')).title()

            cursor.execute(
                sql,
                (
                    product['id'],
                    product['product_name'],
                    product['supplier_name'],
                    product['price'],
                    product['cost'],
                    product['margin'],
                    product['category'],
                )
            )

        conn.commit()
        logging.info("Products successfully loaded to MySQL")
    except Exception as exc:
        conn.rollback()
        logging.error("Failed to load products: %s", exc)
        raise
    finally:
        cursor.close()
        conn.close()


def transform_and_load_orders(**context):
    
    # Transform data order dan load ke MySQL
    orders = context['ti'].xcom_pull(
        task_ids='extract_orders',
        key='orders_data'
    )

    if not orders:
        logging.info("No order data to process")
        return

    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    sql = """
        INSERT INTO fact_orders (
            id, customer_id, product_id,
            total_amount, status, order_date
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            total_amount = VALUES(total_amount),
            status = VALUES(status)
    """

    try:
        for order in orders:
            order['status'] = str(order.get('status', '')).lower()

            if order.get('total_amount', 0) < 0:
                logging.warning(
                    "Negative total_amount for order_id=%s",
                    order['id']
                )
                order['total_amount'] = 0

            cursor.execute(
                sql,
                (
                    order['id'],
                    order['customer_id'],
                    order['product_id'],
                    order['total_amount'],
                    order['status'],
                    order['order_date'],
                )
            )

        conn.commit()
        logging.info("Orders successfully loaded to MySQL")
    except Exception as exc:
        conn.rollback()
        logging.error("Failed to load orders: %s", exc)
        raise
    finally:
        cursor.close()
        conn.close()


# Task Definitions
extract_customers = PythonOperator(
    task_id='extract_customers',
    python_callable=extract_customers_from_postgres,
    dag=dag,
)

extract_products = PythonOperator(
    task_id='extract_products',
    python_callable=extract_products_from_postgres,
    dag=dag,
)

extract_orders = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders_from_postgres,
    dag=dag,
)

load_customers = PythonOperator(
    task_id='load_customers',
    python_callable=transform_and_load_customers,
    dag=dag,
)

load_products = PythonOperator(
    task_id='load_products',
    python_callable=transform_and_load_products,
    dag=dag,
)

load_orders = PythonOperator(
    task_id='load_orders',
    python_callable=transform_and_load_orders,
    dag=dag,
)



# Task Dependencies

extract_customers >> load_customers
extract_products >> load_products
extract_orders >> load_orders
