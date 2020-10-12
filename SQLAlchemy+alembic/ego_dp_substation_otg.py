from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic import op
from sqlalchemy import Table, Column, BigInteger, create_engine, update, select, MetaData, and_, cast, func
import sqlalchemy


# create_engine
engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')
conn = engine.connect()

#alembic
ctx = MigrationContext.configure(conn)
op = Operations(ctx)

#add column
op.add_column('ego_grid_hvmv_substation',
    Column('otg_id', BigInteger),
    schema = 'model_draft'
)

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


#operations with "ego_grid_hvmv_substation" table

#update
hvmv_substation_update = ego_grid_hvmv_substation.update().values(otg_id=otg_ehvhv_bus_data.c.bus_i).where(and_(otg_ehvhv_bus_data.c.base_kv<=110, otg_ehvhv_bus_data.c.osm_substation_id==cast(func.trim(ego_grid_hvmv_substation.c.osm_id, 'nw'), BigInteger)))

#delete 
hvmv_substation_delete = ego_grid_hvmv_substation.delete().where(ego_grid_hvmv_substation.c.otg_id.is_(None))

#execution
conn.execute(hvmv_substation_update)
conn.execute(hvmv_substation_delete)


#operations with "ego_grid_ehv_substation" table

#update
ehv_substation_update = ego_grid_ehv_substation.update().values(otg_id=otg_ehvhv_bus_data.c.bus_i).where(and_(otg_ehvhv_bus_data.c.base_kv>110, otg_ehvhv_bus_data.c.osm_substation_id==cast(func.trim(ego_grid_ehv_substation.c.osm_id, 'nwr'), BigInteger)))

#delete
ehv_substation_delete = ego_grid_ehv_substation.delete().where(ego_grid_ehv_substation.c.otg_id.is_(None))

#execution
conn.execute(ehv_substation_update)
conn.execute(ehv_substation_delete)



