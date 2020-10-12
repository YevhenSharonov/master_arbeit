from sqlalchemy import create_engine
import datetime
import time

def write_into_eleventh_file():

    f = open("/home/sharonov/snakemake/tenth.txt")
    f1 = open("/home/sharonov/snakemake/eleventh.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask create_view_filters_irrelevant_tags:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_filters_irrelevant_tags:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_filters_irrelevant_tags():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.summary CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.summary 
                                               AS
	                                          SELECT *
	                                            FROM model_draft.summary_total
	                                           WHERE dbahn = 'no' AND substation NOT IN ('traction','transition');

                 CREATE INDEX summary_gix ON model_draft.summary USING GIST (polygon);

                 ALTER MATERIALIZED VIEW model_draft.summary OWNER TO oeuser; ''')

    execution_time = time.monotonic() - start_time

create_view_filters_irrelevant_tags()
write_into_eleventh_file()
