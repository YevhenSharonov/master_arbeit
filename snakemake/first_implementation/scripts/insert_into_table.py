from sqlalchemy import create_engine
import datetime

def write_into_third_file():

    f = open("/home/sharonov/snakemake/second.txt")
    f1 = open("/home/sharonov/snakemake/third.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    f1.write("Task insert_into_table: " + str(now) + "\n")
    f.close()
    f1.close()

def insert_into_table():

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/for_test_snakemake')

    with engine.connect() as connection:
        result = connection.execute('''
                 INSERT INTO new_table_snakemake
                 VALUES ('1', '1', 'Info'); ''')

insert_into_table()
write_into_third_file()










