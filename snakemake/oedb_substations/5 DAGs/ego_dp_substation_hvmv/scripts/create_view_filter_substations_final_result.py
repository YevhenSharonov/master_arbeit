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
        f1.write("\nTask create_view_filter_substations_final_result:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_filter_substations_final_result:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_filter_substations_final_result():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.final_result CASCADE;
                         CREATE VIEW model_draft.final_result 
                                  AS
	                             SELECT * 
	                             FROM model_draft.summary_de
	                             WHERE model_draft.summary_de.osm_id 
                                           NOT IN ( SELECT model_draft.substations_to_drop.osm_id FROM model_draft.substations_to_drop);

                 ALTER VIEW model_draft.final_result OWNER TO oeuser; ''')

create_view_filter_substations_final_result()
write_into_sixteenth_file()
