from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# Substation OTG-ID
# Script to assign osmTGmod-id (OTG) to substation.
# 
# __copyright__   = "DLR Institute for Networked Energy Systems"
# __license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
# __url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
# __author__      = "lukasol, C. Matke"


# update model_draft.ego_grid_hvmv_substation table with new column of respective osmtgmod bus_i, 
# fill table with bus_i from osmtgmod
def update_ego_grid_hvmv_substation(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 ALTER TABLE model_draft.ego_grid_hvmv_substation 
	                 ADD COLUMN otg_id bigint;

                 UPDATE model_draft.ego_grid_hvmv_substation
	            SET otg_id = grid.otg_ehvhv_bus_data.bus_i
	           FROM grid.otg_ehvhv_bus_data
	          WHERE grid.otg_ehvhv_bus_data.base_kv <= 110 AND 
                        (SELECT TRIM(leading 'n' 
                        FROM TRIM(leading 'w' 
                        FROM model_draft.ego_grid_hvmv_substation.osm_id))::BIGINT)=grid.otg_ehvhv_bus_data.osm_substation_id;

                 DELETE FROM model_draft.ego_grid_hvmv_substation WHERE otg_id IS NULL; ''')


# do the same with model_draft.ego_grid_ehv_substation
def update_ego_grid_ehv_substation(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 ALTER TABLE model_draft.ego_grid_ehv_substation
	                 ADD COLUMN otg_id bigint;

                 UPDATE model_draft.ego_grid_ehv_substation
	            SET otg_id = grid.otg_ehvhv_bus_data.bus_i
	           FROM grid.otg_ehvhv_bus_data
	          WHERE grid.otg_ehvhv_bus_data.base_kv > 110 AND
                        (SELECT TRIM(leading 'n' 
                        FROM TRIM(leading 'w' FROM TRIM(leading 'r' 
                        FROM model_draft.ego_grid_ehv_substation.osm_id)))::BIGINT)=grid.otg_ehvhv_bus_data.osm_substation_id;

                 DELETE FROM model_draft.ego_grid_ehv_substation WHERE otg_id IS NULL; ''')


dag_params = {
    'dag_id': 'ego_dp_substation_otg',
    'start_date': datetime(2020, 7, 7),
    'schedule_interval': None
}


with DAG(**dag_params) as dag:

    update_ego_grid_hvmv_substation = PythonOperator(
        task_id='update_ego_grid_hvmv_substation',
        python_callable=update_ego_grid_hvmv_substation
    )

    update_ego_grid_ehv_substation = PythonOperator(
        task_id='update_ego_grid_ehv_substation',
        python_callable=update_ego_grid_ehv_substation
    )

    trigger_next_dag = TriggerDagRunOperator(
    trigger_dag_id='ego_dp_substation_hvmv_voronoi', 
    task_id='set_id_as_subst_id'
    )

    update_ego_grid_hvmv_substation >> update_ego_grid_ehv_substation >> trigger_next_dag
