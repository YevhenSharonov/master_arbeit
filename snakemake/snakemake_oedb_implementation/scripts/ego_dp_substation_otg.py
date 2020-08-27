from sqlalchemy import create_engine
import datetime
import time

def write_into_fifth_file():

    f = open("/home/sharonov/snakemake/fourth.txt")
    f1 = open("/home/sharonov/snakemake/fifth.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    f1.write("\nego_dp_substation_otg:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    f.close()
    f1.close()

def ego_dp_substation_otg():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','grid','otg_ehvhv_bus_data','ego_dp_substation_otg.sql',' ');
                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','model_draft','ego_grid_hvmv_substation','ego_dp_substation_otg.sql',' ');
                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','model_draft','ego_grid_ehv_substation','ego_dp_substation_otg.sql',' ');


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


                 SELECT scenario_log('eGo_DP', 'v0.4.5','output','model_draft','ego_grid_hvmv_substation','ego_dp_substation_otg.sql',' ');


                 ALTER TABLE model_draft.ego_grid_ehv_substation
	                 ADD COLUMN otg_id bigint;


                 UPDATE model_draft.ego_grid_ehv_substation
	            SET otg_id = grid.otg_ehvhv_bus_data.bus_i
	           FROM grid.otg_ehvhv_bus_data
	          WHERE grid.otg_ehvhv_bus_data.base_kv > 110 AND
                        (SELECT TRIM(leading 'n' 
                        FROM TRIM(leading 'w' FROM TRIM(leading 'r' 
                        FROM model_draft.ego_grid_ehv_substation.osm_id)))::BIGINT)=grid.otg_ehvhv_bus_data.osm_substation_id;


                 DELETE FROM model_draft.ego_grid_ehv_substation WHERE otg_id IS NULL;


                 SELECT scenario_log('eGo_DP', 'v0.4.5','output','model_draft','ego_grid_ehv_substation','ego_dp_substation_otg.sql',' '); ''')

    execution_time = time.monotonic() - start_time


ego_dp_substation_otg()
write_into_fifth_file()
