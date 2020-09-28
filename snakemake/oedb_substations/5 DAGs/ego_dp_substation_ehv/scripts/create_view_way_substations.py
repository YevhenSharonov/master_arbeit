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
        f1.write("\nTask create_view_way_substations:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_way_substations:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_way_substations():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.way_substations CASCADE;
                         CREATE VIEW model_draft.way_substations 
                                  AS
	                             SELECT openstreetmap.osm_deu_ways.id, openstreetmap.osm_deu_ways.tags, openstreetmap.osm_deu_polygon.geom
	                               FROM openstreetmap.osm_deu_ways JOIN openstreetmap.osm_deu_polygon 
                                            ON openstreetmap.osm_deu_ways.id = openstreetmap.osm_deu_polygon.osm_id
	                              WHERE hstore(openstreetmap.osm_deu_ways.tags)->'power' in ('substation','sub_station','station')
	                              UNION
	                             SELECT openstreetmap.osm_deu_ways.id, openstreetmap.osm_deu_ways.tags, openstreetmap.osm_deu_line.geom
	                               FROM openstreetmap.osm_deu_ways JOIN openstreetmap.osm_deu_line 
                                            ON openstreetmap.osm_deu_ways.id = openstreetmap.osm_deu_line.osm_id
	                              WHERE hstore(openstreetmap.osm_deu_ways.tags)->'power' in ('substation','sub_station','station');

                 ALTER VIEW model_draft.way_substations OWNER TO oeuser; ''')

create_view_way_substations()
write_into_third_file()
