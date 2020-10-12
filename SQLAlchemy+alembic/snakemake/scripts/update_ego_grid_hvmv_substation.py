from sqlalchemy import create_engine
import datetime
import time
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic import op
from sqlalchemy import Table, Column, BigInteger, create_engine, update, select, MetaData, and_, cast, func
import sqlalchemy

def write_into_second_file():

    f = open("/home/sharonov/snakemake/first.txt")
    f1 = open("/home/sharonov/snakemake/second.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask update_ego_grid_hvmv_substation:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask update_ego_grid_hvmv_substation:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def update_ego_grid_hvmv_substation():

    global execution_time
    start_time = time.monotonic()

# create_engine
    engine = create_engine('postgresql://postgres:07041995jsh@localhost:5432/oedb')
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

    execution_time = time.monotonic() - start_time

update_ego_grid_hvmv_substation()
write_into_second_file()
