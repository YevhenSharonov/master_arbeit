from sqlalchemy import create_engine
import datetime
import time

def write_into_tenth_file():

    f = open("/home/sharonov/snakemake/ninth.txt")
    f1 = open("/home/sharonov/snakemake/tenth.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask create_index_gist:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_index_gist:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_index_gist():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 CREATE INDEX summary_hoes_gix ON model_draft.summary_hoes USING GIST (polygon); ''')

create_index_gist()
write_into_tenth_file()
