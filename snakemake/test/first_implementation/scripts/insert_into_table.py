from sqlalchemy import create_engine
import datetime
import time

def write_into_third_file():

    f = open("/home/sharonov/snakemake/second.txt")
    f1 = open("/home/sharonov/snakemake/third.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    f1.write("\nTask insert_into_table:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    f.close()
    f1.close()

def insert_into_table():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/for_test_snakemake')

    with engine.connect() as connection:
        result = connection.execute('''
                 INSERT INTO new_table_snakemake
                 VALUES ('111', '1111', 'Data'); ''')

    execution_time = time.monotonic() - start_time

insert_into_table()
write_into_third_file()










