from sqlalchemy import create_engine
import datetime
import time

def write_into_fourth_file():

    f = open("/home/sharonov/snakemake/third.txt")
    f1 = open("/home/sharonov/snakemake/fourth.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask create_view_way_substations_with_hoes:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_way_substations_with_hoes:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_way_substations_with_hoes():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.way_substations_with_hoes CASCADE;
                         CREATE VIEW model_draft.way_substations_with_hoes 
                                  AS
	                             SELECT * 
	                               FROM model_draft.way_substations
	                              WHERE '220000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';')) 
		                            OR '380000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';'));

                 ALTER VIEW model_draft.way_substations_with_hoes OWNER TO oeuser; ''')

    execution_time = time.monotonic() - start_time

create_view_way_substations_with_hoes()
write_into_fourth_file()
