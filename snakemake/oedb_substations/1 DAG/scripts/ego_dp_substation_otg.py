from sqlalchemy import create_engine
import datetime
import time

def write_into_fourth_file():

    f = open("/home/sharonov/snakemake/third.txt")
    f1 = open("/home/sharonov/snakemake/fourth.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask ego_dp_substation_otg:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask ego_dp_substation_otg:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def ego_dp_substation_otg():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

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


                 DELETE FROM model_draft.ego_grid_hvmv_substation WHERE otg_id IS NULL;


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

    execution_time = time.monotonic() - start_time


ego_dp_substation_otg()
write_into_fourth_file()
