from sqlalchemy import create_engine
import datetime
import time

def write_into_thirteenth_file():

    f = open("/home/sharonov/snakemake/twelfth.txt")
    f1 = open("/home/sharonov/snakemake/thirteenth.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask create_view_buffer_75m_around_polygons:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_buffer_75m_around_polygons:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_buffer_75m_around_polygons():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75 CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.buffer_75 
                                               AS
	                                          SELECT osm_id, ST_Area(ST_Transform(model_draft.summary_de.polygon,4326)) as area, 
                                                         ST_Buffer_Meters(ST_Transform(model_draft.summary_de.polygon,4326), 75) as buffer_75
	                                            FROM model_draft.summary_de;

                 ALTER MATERIALIZED VIEW model_draft.buffer_75 OWNER TO oeuser; ''')

    execution_time = time.monotonic() - start_time

create_view_buffer_75m_around_polygons()
write_into_thirteenth_file()
