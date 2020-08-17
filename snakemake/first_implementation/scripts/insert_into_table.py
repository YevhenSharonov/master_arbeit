from sqlalchemy import create_engine

def write_into_third_file():

    f = open("/home/sharonov/snakemake/second.txt")
    f1 = open("/home/sharonov/snakemake/third.txt", "a")
    for x in f.readlines():
        f1.write(x)
    f1.write("\nThis one is new also")
    f.close()
    f1.close()

def insert_into_table():

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/for_test_snakemake')

    with engine.connect() as connection:
        result = connection.execute('''
                 INSERT INTO new_table_snakemake
                 VALUES ('1', '11', 'First'); ''')

write_into_third_file()
insert_into_table()









