from sqlalchemy import create_engine
import datetime
import time

def write_into_eighth_file():

    f = open("/home/sharonov/snakemake/seventh.txt")
    f1 = open("/home/sharonov/snakemake/eighth.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask create_view_way_substations_without_110kv_intersected_by_110kv_line:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_way_substations_without_110kv_intersected_by_110kv_line:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_way_substations_without_110kv_intersected_by_110kv_line():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.way_substations_without_110kV_intersected_by_110kV_line CASCADE;
                         CREATE VIEW model_draft.way_substations_without_110kV_intersected_by_110kV_line 
                                  AS
	                             SELECT DISTINCT model_draft.way_substations_without_110kV.* 
	                               FROM model_draft.way_substations_without_110kV, model_draft.way_lines_110kV
	                              WHERE ST_Contains(model_draft.way_substations_without_110kV.geom,ST_StartPoint(model_draft.way_lines_110kV.geom)) 
		                            OR ST_Contains(model_draft.way_substations_without_110kV.geom,ST_EndPoint(model_draft.way_lines_110kV.geom));

                 ALTER VIEW model_draft.way_substations_without_110kV_intersected_by_110kV_line OWNER TO oeuser; ''')

    execution_time = time.monotonic() - start_time

create_view_way_substations_without_110kv_intersected_by_110kv_line()
write_into_eighth_file()
