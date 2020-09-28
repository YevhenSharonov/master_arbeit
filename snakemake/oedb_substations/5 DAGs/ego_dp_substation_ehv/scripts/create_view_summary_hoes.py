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
        f1.write("\nTask create_view_summary_hoes:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_summary_hoes:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_summary_hoes():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.summary_hoes CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.summary_hoes 
                                               AS
	                                          SELECT *
	                                            FROM model_draft.summary_total_hoes
	                                           WHERE dbahn = 'no' AND substation NOT IN ('traction','transition');

                 ALTER MATERIALIZED VIEW model_draft.summary_hoes OWNER TO oeuser; ''')

create_view_summary_hoes()
write_into_ninth_file()
