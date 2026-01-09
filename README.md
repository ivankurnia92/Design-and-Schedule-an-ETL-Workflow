# ETL Pipeline PostgreSQL ke MySQL dengan Apache Airflow

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-blue)
![MySQL](https://img.shields.io/badge/MySQL-8+-orange)
![Python](https://img.shields.io/badge/Python-3.8+-green)

## 📌 Gambaran Umum
Repositori ini berisi implementasi **ETL Pipeline (Extract, Transform, Load)** menggunakan **Apache Airflow** untuk memindahkan data secara inkremental dari **PostgreSQL** ke **MySQL**.

Pipeline ini dirancang menyerupai praktik produksi dan menampilkan:
- Struktur DAG Airflow yang rapi
- Proses data inkremental
- Transformasi data berbasis aturan bisnis
- Proses loading idempotent menggunakan UPSERT
- Logging dan error handling yang baik

---

## 🏗️ Arsitektur

```text
PostgreSQL (Sumber)
   ├── raw_data.customers
   ├── raw_data.products
   ├── raw_data.suppliers
   └── raw_data.orders
            |
            v
      Apache Airflow
   (Extract → Transform → Load)
            |
            v
MySQL (Data Warehouse)
   ├── dim_customers
   ├── dim_products
   └── fact_orders



| Konfigurasi | Keterangan                                    |
| ----------- | --------------------------------------------- |
| DAG ID      | `postgres_to_mysql_etl`                       |
| Jadwal      | Setiap 6 jam                                  |
| Start Date  | 1 hari yang lalu                              |
| Catchup     | Dinonaktifkan                                 |
| Owner       | `data-engineering-team`                       |
| Retry       | 2 kali                                        |
| Retry Delay | 5 menit                                       |
| Tags        | `etl`, `postgresql`, `mysql`, `data-pipeline` |

