from sqlalchemy import create_engine
import datetime

def write_into_second_file():

    f = open("/home/sharonov/snakemake/first.txt")
    f1 = open("/home/sharonov/snakemake/second.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    f1.write("\nTask create_table: " + str(now) + "\n")
    f.close()
    f1.close()

def create_table():

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/for_test_snakemake')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP TABLE IF EXISTS new_table_snakemake;
                 CREATE TABLE new_table_snakemake(
                 custom_id INTEGER NOT NULL,
                 name INTEGER NOT NULL,
                 user_id VARCHAR(50) NOT NULL); ''')


create_table()
write_into_second_file()










