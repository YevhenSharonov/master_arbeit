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
        f1.write("\nTask set_id_as_subst_id:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask set_id_as_subst_id:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def set_id_as_subst_id():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                           ALTER TABLE model_draft.ego_grid_hvmv_substation
	         DROP COLUMN IF EXISTS ags_0 CASCADE,
	                    ADD COLUMN ags_0 text,
	         DROP COLUMN IF EXISTS geom CASCADE,
	                    ADD COLUMN geom geometry(Point,3035);

                 UPDATE model_draft.ego_grid_hvmv_substation t1
     	            SET geom = ST_TRANSFORM(t1.point,3035)
	           FROM model_draft.ego_grid_hvmv_substation t2
	          WHERE t1.subst_id = t2.subst_id;

                 CREATE INDEX ego_grid_hvmv_substation_geom_idx
	                 ON model_draft.ego_grid_hvmv_substation USING GIST (geom); ''')

set_id_as_subst_id()
write_into_second_file()
