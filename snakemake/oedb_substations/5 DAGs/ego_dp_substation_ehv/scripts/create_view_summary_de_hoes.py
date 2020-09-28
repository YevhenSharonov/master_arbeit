from sqlalchemy import create_engine
import datetime
import time

def write_into_eleventh_file():

    f = open("/home/sharonov/snakemake/tenth.txt")
    f1 = open("/home/sharonov/snakemake/eleventh.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask create_view_summary_de_hoes:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_summary_de_hoes:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_summary_de_hoes():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.summary_de_hoes CASCADE;
                         CREATE VIEW model_draft.summary_de_hoes 
                                  AS
	                             SELECT *
	                               FROM model_draft.summary_hoes, boundaries.bkg_vg250_1_sta_union_mview as vg
	                              WHERE ST_Transform(vg.geom,4326) && model_draft.summary_hoes.polygon 
	                                    AND ST_CONTAINS(ST_Transform(vg.geom,4326),model_draft.summary_hoes.polygon);

                 ALTER VIEW model_draft.summary_de_hoes OWNER TO oeuser; ''')

create_view_summary_de_hoes()
write_into_eleventh_file()
