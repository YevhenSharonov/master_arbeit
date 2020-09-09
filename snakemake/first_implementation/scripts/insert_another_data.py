from sqlalchemy import create_engine
import datetime
import time

def write_into_fourth_file():

    f = open("/home/sharonov/snakemake/third.txt")
    f1 = open("/home/sharonov/snakemake/log_file.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    f1.write("\nTask insert_another_data:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    f.close()
    f1.close()

def insert_another_data():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/for_test_snakemake')

    with engine.connect() as connection:
        result = connection.execute('''
                 INSERT INTO new_table_snakemake
                 VALUES ('222', '2222', 'Moredata'); ''')

    execution_time = time.monotonic() - start_time



insert_another_data()
write_into_fourth_file()









