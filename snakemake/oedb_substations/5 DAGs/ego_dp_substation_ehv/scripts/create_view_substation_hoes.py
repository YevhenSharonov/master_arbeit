from sqlalchemy import create_engine
import datetime
import time

def write_into_seventh_file():

    f = open("/home/sharonov/snakemake/sixth.txt")
    f1 = open("/home/sharonov/snakemake/seventh.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask create_view_substation_hoes:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_substation_hoes:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_substation_hoes():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.substation_hoes CASCADE;
                         CREATE VIEW model_draft.substation_hoes 
                                  AS
	                             SELECT *,
		                            'http://www.osm.org/relation/'|| model_draft.relation_substations_with_hoes.id as osm_www,
		                            'r'|| model_draft.relation_substations_with_hoes.id as osm_id,
		                            '1'::smallint as status
	                               FROM model_draft.relation_substations_with_hoes
	                              UNION
	                             SELECT *,
		                            'http://www.osm.org/way/'|| model_draft.way_substations_with_hoes.id as osm_www,
		                            'w'|| model_draft.way_substations_with_hoes.id as osm_id,
		                            '2'::smallint as status
	                               FROM model_draft.way_substations_with_hoes
	                              UNION 
	                             SELECT *,
		                            'http://www.osm.org/node/'|| model_draft.node_substations_with_hoes.id as osm_www,
		                            'n'|| model_draft.node_substations_with_hoes.id as osm_id,
		                            '4'::smallint as status
	                               FROM model_draft.node_substations_with_hoes;

                 ALTER VIEW model_draft.substation_hoes OWNER TO oeuser; ''')

    execution_time = time.monotonic() - start_time

create_view_substation_hoes()
write_into_seventh_file()
