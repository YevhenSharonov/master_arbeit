from sqlalchemy import create_engine
import datetime
import time

def write_into_ninth_file():

    f = open("/home/sharonov/snakemake/eighth.txt")
    f1 = open("/home/sharonov/snakemake/ninth.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask drop_create_view_substation_110kv:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask drop_create_view_substation_110kv:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def drop_create_view_substation_110kv():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.substation_110kV CASCADE;
                         CREATE VIEW model_draft.substation_110kV 
                                  AS
	                             SELECT *,
		                            'http://www.osm.org/way/'|| model_draft.way_substations_with_110kV.id as osm_www,
		                            'w'|| model_draft.way_substations_with_110kV.id as osm_id,
		                            '1'::smallint as status
	                               FROM model_draft.way_substations_with_110kV
	                              UNION 
	                             SELECT *,
		                            'http://www.osm.org/way/'|| model_draft.way_substations_without_110kV_intersected_by_110kV_line.id as osm_www,
		                            'w'|| model_draft.way_substations_without_110kV_intersected_by_110kV_line.id as osm_id,
		                            '2'::smallint as status
	                               FROM model_draft.way_substations_without_110kV_intersected_by_110kV_line
	                              UNION 
	                             SELECT *,
		                            'http://www.osm.org/node/'|| model_draft.node_substations_with_110kV.id as osm_www,
		                            'n'|| model_draft.node_substations_with_110kV.id as osm_id,
		                            '3'::smallint as status
	                               FROM model_draft.node_substations_with_110kV;

                 ALTER VIEW model_draft.substation_110kV OWNER TO oeuser; ''')

drop_create_view_substation_110kv()
write_into_ninth_file()
