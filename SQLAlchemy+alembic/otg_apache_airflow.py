from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic import op
from sqlalchemy import Table, Column, BigInteger, create_engine, update, select, MetaData, and_, cast, func
import sqlalchemy

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
    conn = engine.connect()
    
#alembic
    ctx = MigrationContext.configure(conn)
    op = Operations(ctx)

#add column
    op.add_column('ego_grid_hvmv_substation',
        Column('otg_id', BigInteger),
        schema = 'model_draft'
    )

# load the tables
    meta = MetaData()
    conn.execute("SET search_path TO model_draft, grid, public")

    ego_grid_hvmv_substation = Table('ego_grid_hvmv_substation', meta, autoload=True, autoload_with=conn, postgresql_ignore_search_path=True)
    ego_grid_ehv_substation = Table('ego_grid_ehv_substation', meta, autoload=True, autoload_with=conn, postgresql_ignore_search_path=True)
    otg_ehvhv_bus_data = Table('otg_ehvhv_bus_data', meta, autoload=True, autoload_with=conn, postgresql_ignore_search_path=True)


#operations with "ego_grid_hvmv_substation" table

#update
    hvmv_substation_update = ego_grid_hvmv_substation.update().values(otg_id=otg_ehvhv_bus_data.c.bus_i).where(
                                                      and_(
                                                           otg_ehvhv_bus_data.c.base_kv<=110, 
                                                           otg_ehvhv_bus_data.c.osm_substation_id==cast(func.trim(ego_grid_hvmv_substation.c.osm_id, 'nw'), BigInteger)
                                                      )
                             )

#delete 
    hvmv_substation_delete = ego_grid_hvmv_substation.delete().where(
                                                      ego_grid_hvmv_substation.c.otg_id.is_(None)
                             )

#execution
    conn.execute(hvmv_substation_update)
    conn.execute(hvmv_substation_delete)



# do the same with model_draft.ego_grid_ehv_substation
def update_ego_grid_ehv_substation(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    conn = engine.connect()
    
#alembic
    ctx = MigrationContext.configure(conn)
    op = Operations(ctx)

#add column
    op.add_column('ego_grid_ehv_substation',
        Column('otg_id', BigInteger),
        schema = 'model_draft'
    )

# load the tables
    meta = MetaData()
    conn.execute("SET search_path TO model_draft, grid, public")

    ego_grid_hvmv_substation = Table('ego_grid_hvmv_substation', meta, autoload=True, autoload_with=conn, postgresql_ignore_search_path=True)
    ego_grid_ehv_substation = Table('ego_grid_ehv_substation', meta, autoload=True, autoload_with=conn, postgresql_ignore_search_path=True)
    otg_ehvhv_bus_data = Table('otg_ehvhv_bus_data', meta, autoload=True, autoload_with=conn, postgresql_ignore_search_path=True)

#operations with "ego_grid_ehv_substation" table

#update
    ehv_substation_update = ego_grid_ehv_substation.update().values(otg_id=otg_ehvhv_bus_data.c.bus_i).where(
                                                    and_(
                                                         otg_ehvhv_bus_data.c.base_kv>110, 
                                                         otg_ehvhv_bus_data.c.osm_substation_id==cast(func.trim(ego_grid_ehv_substation.c.osm_id, 'nwr'), BigInteger)
                                                    )
                            )

#delete
    ehv_substation_delete = ego_grid_ehv_substation.delete().where(
                                                    ego_grid_ehv_substation.c.otg_id.is_(None)
                            )

#execution
    conn.execute(ehv_substation_update)
    conn.execute(ehv_substation_delete)




dag_params = {
    'dag_id': 'ego_dp_substation_otg_with_alembic',
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


    update_ego_grid_hvmv_substation >> update_ego_grid_ehv_substation
