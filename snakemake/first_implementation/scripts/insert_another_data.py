from sqlalchemy import create_engine
import datetime


def write_into_log_file():

    f = open("/home/sharonov/snakemake/third.txt")
    f1 = open("/home/sharonov/snakemake/log_file.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    f1.write("Task insert_another_data: " + str(now) + "\n")
    f.close()
    f1.close()

def insert_another_data():

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/for_test_snakemake')

    with engine.connect() as connection:
        result = connection.execute('''
                 INSERT INTO new_table_snakemake
                 VALUES ('2', '2', 'Moreinfo'); ''')


insert_another_data()
write_into_log_file()










