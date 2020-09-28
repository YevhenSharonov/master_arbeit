from sqlalchemy import create_engine
import datetime
import time

def write_into_second_file():

    f = open("/home/sharonov/snakemake/first.txt")
    f1 = open("/home/sharonov/snakemake/second.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask create_table_ego_grid_ehv_substation:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_table_ego_grid_ehv_substation:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_table_ego_grid_ehv_substation():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP TABLE IF EXISTS model_draft.ego_grid_ehv_substation CASCADE;
                         CREATE TABLE model_draft.ego_grid_ehv_substation (
	                     subst_id   serial NOT NULL,
	                     lon        float NOT NULL,
	                     lat        float NOT NULL,
	                     point      geometry(Point,4326) NOT NULL,
	                     polygon    geometry NOT NULL,	
	                     voltage    text,
	                     power_type text,
	                     substation text,
	                     osm_id     text PRIMARY KEY NOT NULL,
	                     osm_www    text NOT NULL,
	                     frequency  text,
	                     subst_name text,
	                     ref        text,
	                     operator   text,
	                     dbahn      text,
	                     status     smallint NOT NULL);

                 ALTER TABLE model_draft.ego_grid_ehv_substation OWNER TO oeuser;
     
                 COMMENT ON TABLE  model_draft.ego_grid_ehv_substation IS '{
	             "comment": "eGoDP - Versioning table",
	             "version": "v0.4.5" }' ; ''')

create_table_ego_grid_ehv_substation()
write_into_second_file()
