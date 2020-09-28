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
        f1.write("\nTask create_view_relation_substations_with_hoes:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_relation_substations_with_hoes:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_relation_substations_with_hoes():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.relation_substations_with_hoes CASCADE;
                         CREATE VIEW model_draft.relation_substations_with_hoes 
                                  AS
	                             SELECT openstreetmap.osm_deu_rels.id, openstreetmap.osm_deu_rels.tags, relation_geometry(openstreetmap.osm_deu_rels.members) as way
	                               FROM openstreetmap.osm_deu_rels 
	                              WHERE '220000' = ANY( string_to_array(hstore(openstreetmap.osm_deu_rels.tags)->'voltage',';')) 
		                            AND hstore(openstreetmap.osm_deu_rels.tags)->'power' in ('substation','sub_station','station') 
		                            OR '380000' = ANY( string_to_array(hstore(openstreetmap.osm_deu_rels.tags)->'voltage',';')) 
		                            AND hstore(openstreetmap.osm_deu_rels.tags)->'power' in ('substation','sub_station','station');

                 ALTER VIEW model_draft.relation_substations_with_hoes OWNER TO oeuser; ''')

create_view_relation_substations_with_hoes()
write_into_sixth_file()
