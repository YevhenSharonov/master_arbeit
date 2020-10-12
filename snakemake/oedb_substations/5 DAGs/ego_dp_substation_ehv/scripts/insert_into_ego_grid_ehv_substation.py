from sqlalchemy import create_engine
import datetime
import time

def write_into_sixteenth_file():

    f = open("/home/sharonov/snakemake/fifteenth.txt")
    f1 = open("/home/sharonov/snakemake/sixteenth.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask insert_into_ego_grid_ehv_substation:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask insert_into_ego_grid_ehv_substation:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def insert_into_ego_grid_ehv_substation():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 INSERT INTO model_draft.ego_grid_ehv_substation (lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status)
	              SELECT lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status
	                FROM model_draft.final_result_hoes; ''')

    execution_time = time.monotonic() - start_time

insert_into_ego_grid_ehv_substation()
write_into_sixteenth_file()
