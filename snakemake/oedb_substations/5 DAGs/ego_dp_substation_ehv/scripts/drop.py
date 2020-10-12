from sqlalchemy import create_engine
import datetime
import time

def write_into_log_file():

    f = open("/home/sharonov/snakemake/sixteenth.txt")
    f1 = open("/home/sharonov/snakemake/log_file.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask drop:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask drop:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def drop():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                              DROP VIEW IF EXISTS model_draft.final_result_hoes CASCADE;
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.substations_to_drop_hoes CASCADE;
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75_hoes CASCADE;
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75_a_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.summary_de_hoes CASCADE;
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.summary_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.summary_total_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.substation_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.relation_substations_with_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.node_substations_with_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.way_substations_with_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.way_substations CASCADE; ''')

    execution_time = time.monotonic() - start_time

drop()
write_into_log_file()
