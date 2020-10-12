from sqlalchemy import create_engine
import datetime
import time

def write_into_third_file():

    f = open("/home/sharonov/snakemake/second.txt")
    f1 = open("/home/sharonov/snakemake/third.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask gemeindeschluessel:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask gemeindeschluessel:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def gemeindeschluessel():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 UPDATE model_draft.ego_grid_hvmv_substation AS t1
	            SET ags_0 = t2.ags_0
	           FROM (SELECT	sub.subst_id AS subst_id, vg.ags_0 AS ags_0
		        FROM model_draft.ego_grid_hvmv_substation AS sub, model_draft.ego_boundaries_bkg_vg250_6_gem_clean AS vg
		        WHERE vg.geom && sub.geom AND ST_CONTAINS(vg.geom,sub.geom)
		        ) AS t2
	          WHERE t1.subst_id = t2.subst_id; ''')

    execution_time = time.monotonic() - start_time

gemeindeschluessel()
write_into_third_file()
