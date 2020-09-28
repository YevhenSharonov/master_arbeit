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
        f1.write("\nTask create_view_eiliminate_smaller_substations:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_eiliminate_smaller_substations:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_eiliminate_smaller_substations():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.substations_to_drop CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.substations_to_drop 
                                               AS
	                                          SELECT DISTINCT
	                                                 (CASE WHEN model_draft.buffer_75.area < model_draft.buffer_75_a.area_a 
                                                               THEN model_draft.buffer_75.osm_id 
                                                               ELSE model_draft.buffer_75_a.osm_id END) as osm_id,
	                                                 (CASE WHEN model_draft.buffer_75.area < model_draft.buffer_75_a.area_a 
                                                               THEN model_draft.buffer_75.area 
                                                               ELSE model_draft.buffer_75_a.area_a END) as area,
	                                                 (CASE WHEN model_draft.buffer_75.area < model_draft.buffer_75_a.area_a 
                                                               THEN model_draft.buffer_75.buffer_75 
                                                               ELSE model_draft.buffer_75_a.buffer_75_a END) as buffer
	                                            FROM model_draft.buffer_75, model_draft.buffer_75_a
	                                           WHERE ST_Intersects(model_draft.buffer_75.buffer_75, model_draft.buffer_75_a.buffer_75_a)
	                                                 AND NOT model_draft.buffer_75.osm_id = model_draft.buffer_75_a.osm_id;
     
                 ALTER MATERIALIZED VIEW model_draft.substations_to_drop OWNER TO oeuser; ''')

create_view_eiliminate_smaller_substations()
write_into_fifteenth_file()
