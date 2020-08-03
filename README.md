# Installation (Apache Airflow)

1. install conda 
2. install apache-airflow ("airflow initdb" after installation) 
3. install postgres-12
4. install PostGIS
5. install pscycopg2
6. sudo apt-get install -y build-essential 

# Usage

1. Create user "airflow" and database "airflow" with password in PostgreSQL:
* CREATE USER airflow with password 'password';
* CREATE DATABASE airflow;
* GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow; 

2. Connect Apache Airflow to PostgreSQL. 
* In airflow.cfg file look for "sql_alchemy_conn" and update it to point to your PostgreSQL server: sql_alchemy_conn = postgresql+psycopg2://user:pass@hostadress:port/database
* Example: sql_alchemy_conn = postgresql+psycopg2://airflow:password@localhost:5432/airflow
* Again "airflow initdb"

3. Copy DAGs in "dags" folder and start the Apache Airflow from terminal with "airflow webserver -p 8080" and "airflow scheduler"

4. Creat new connection to your postgres

5. Trigger DAG
