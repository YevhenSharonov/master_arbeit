from sqlalchemy import create_engine
import datetime
import time

def write_into_sixth_file():

    f = open("/home/sharonov/snakemake/fifth.txt")
    f1 = open("/home/sharonov/snakemake/sixth.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask create_view_node_substation_110kv:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_node_substation_110kv:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_node_substation_110kv():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.node_substations_with_110kV CASCADE;
                         CREATE VIEW model_draft.node_substations_with_110kV 
                                  AS
	                             SELECT openstreetmap.osm_deu_nodes.id, openstreetmap.osm_deu_nodes.tags, openstreetmap.osm_deu_point.geom
	                               FROM openstreetmap.osm_deu_nodes JOIN openstreetmap.osm_deu_point ON openstreetmap.osm_deu_nodes.id = openstreetmap.osm_deu_point.osm_id
	                              WHERE '110000' = ANY( string_to_array(hstore(openstreetmap.osm_deu_nodes.tags)->'voltage',';')) 
		                            AND hstore(openstreetmap.osm_deu_nodes.tags)->'power' in ('substation','sub_station','station') 
		                            OR '60000' = ANY( string_to_array(hstore(openstreetmap.osm_deu_nodes.tags)->'voltage',';')) 
		                            AND hstore(openstreetmap.osm_deu_nodes.tags)->'power' in ('substation','sub_station','station');

                 ALTER VIEW model_draft.node_substations_with_110kV OWNER TO oeuser; ''')

create_view_node_substation_110kv()
write_into_sixth_file()
