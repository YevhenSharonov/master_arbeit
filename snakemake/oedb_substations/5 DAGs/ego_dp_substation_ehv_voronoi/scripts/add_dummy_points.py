from sqlalchemy import create_engine
import datetime
import time

def write_into_second_file():

    f = open("/home/sharonov/snakemake/first.txt")
    f1 = open("/home/sharonov/snakemake/second.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask add_dummy_points:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask add_dummy_points:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def add_dummy_points():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 INSERT INTO model_draft.ego_grid_ehv_substation (subst_name, point, subst_id, otg_id, lon, lat, polygon, osm_id, osm_www, status)
                      SELECT 'DUMMY', ST_TRANSFORM(geom,4326), subst_id, subst_id, ST_X (ST_Transform (geom, 4326)), ST_Y (ST_Transform (geom, 4326)), 
                             'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', 'dummy'||row_number() OVER(), 'dummy', 0
                        FROM model_draft.ego_grid_hvmv_substation_dummy; ''')

    execution_time = time.monotonic() - start_time

add_dummy_points()
write_into_second_file()
