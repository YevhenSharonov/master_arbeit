# Apache Airflow

## Installation

1. install conda 
2. install apache-airflow ("airflow initdb" after installation) 
3. install postgres-12
4. install PostGIS
5. install pscycopg2
6. sudo apt-get install -y build-essential 

## Usage

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



# Snakemake

## Installation

1. install conda 
2. conda install -c conda-forge mamba
3. mamba create -c conda-forge -c bioconda -n snakemake snakemake
4. conda activate snakemake
5. install pscycopg2
6. install SQLAlchemy

## Usage

1. Create folder "snakemake"
2. Copy "scripts" folder, first.txt and Snakefile into "snakemake" folder
3. Open scripts
* Change path to files in each script
* Change '12345678' password for postgres to your password 
4. Open "snakemake" folder in Terminal
5. Execute "conda activate snakemake"
6. Different options:
* "snakemake -n" for performing a dry run of Snakemake 
* "snakemake --dag | dot | display" for creating a representation of the DAG
* "snakemake --cores 4" for executing the workflow 
 
