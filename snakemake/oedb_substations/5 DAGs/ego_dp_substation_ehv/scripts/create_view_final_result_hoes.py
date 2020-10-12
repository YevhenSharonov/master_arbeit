from sqlalchemy import create_engine
import datetime
import time

def write_into_fifteenth_file():

    f = open("/home/sharonov/snakemake/fourteenth.txt")
    f1 = open("/home/sharonov/snakemake/fifteenth.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask create_view_final_result_hoes:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_final_result_hoes:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_final_result_hoes():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.final_result_hoes CASCADE;
                         CREATE VIEW model_draft.final_result_hoes 
                                  AS
	                             SELECT * 
	                               FROM model_draft.summary_de_hoes
	                              WHERE model_draft.summary_de_hoes.osm_id 
                                            NOT IN 
                                            (SELECT model_draft.substations_to_drop_hoes.osm_id 
                                            FROM model_draft.substations_to_drop_hoes);

                 ALTER VIEW model_draft.final_result_hoes OWNER TO oeuser; ''')

    execution_time = time.monotonic() - start_time

create_view_final_result_hoes()
write_into_fifteenth_file()
