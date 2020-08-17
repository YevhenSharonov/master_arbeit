from sqlalchemy import create_engine

def write_into_fourth_file():

    f = open("/home/sharonov/snakemake/third.txt")
    f1 = open("/home/sharonov/snakemake/fourth.txt", "a")
    for x in f.readlines():
        f1.write(x)
    f1.write("\nNew one")
    f.close()
    f1.close()

def insert_another_data():

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/for_test_snakemake')

    with engine.connect() as connection:
        result = connection.execute('''
                 INSERT INTO new_table_snakemake
                      VALUES ('2', '22', 'Second'); ''')

write_into_fourth_file()
insert_another_data()









