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

3. In airflow.cfg file look for "executor" and change it to "executor = LocalExecutor"

4. Copy DAGs in "dags" folder and start the Apache Airflow from terminal with "airflow webserver -p 8080" and "airflow scheduler" in another terminal

5. Open "http://localhost:8080/" in web browser

6. Creat new connection "postgres_oedb" to your postgres
* On main page open "Admin --> Connections"
* Choose "Create"
* 
* Conn Id: postgres_oedb
* Conn Type: postgres
* Host: localhost
* Schema: oedb
* Login: postgres
* Password: your password for postgres
* Port: 5432
* 
* "Save"

7. Trigger DAG
* On main page turn on all "ego_dp_substatios" DAGs
* Click on first DAG (ego_dp_substation_hvmv) and trigger this DAG by "Trigger DAG" button
