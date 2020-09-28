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
        f1.write("\nTask create_view_summary_total:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_view_summary_total:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_view_summary_total():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.summary_total CASCADE;
                         CREATE VIEW model_draft.summary_total 
                                  AS
	                             SELECT ST_X(ST_Centroid(ST_Transform(substation.geom,4326))) as lon,
		                            ST_Y(ST_Centroid(ST_Transform(substation.geom,4326))) as lat,
		                            ST_Centroid(ST_Transform(substation.geom,4326)) as point,
		                            ST_Transform(substation.geom,4326) as polygon,
		                            (CASE WHEN hstore(substation.tags)->'voltage' <> '' 
                                                  THEN hstore(substation.tags)->'voltage' 
                                                  ELSE '110000' END) as voltage, 
		                            hstore(substation.tags)->'power' as power_type, 
		                            (CASE WHEN hstore(substation.tags)->'substation' <> '' 
                                                  THEN hstore(substation.tags)->'substation' 
                                                  ELSE 'NA' END) as substation, 
		                            substation.osm_id as osm_id, 
                                            osm_www,
		                            (CASE WHEN hstore(substation.tags)->'frequency' <> '' 
                                                  THEN hstore(substation.tags)->'frequency' 
                                                  ELSE 'NA' END) as frequency,
		                            (CASE WHEN hstore(substation.tags)->'name' <> '' 
                                                  THEN hstore(substation.tags)->'name' 
                                                  ELSE 'NA' END) as subst_name, 
		                            (CASE WHEN hstore(substation.tags)->'ref' <> '' 
                                                  THEN hstore(substation.tags)->'ref' 
                                                  ELSE 'NA' END) as ref, 
		                            (CASE WHEN hstore(substation.tags)->'operator' <> '' 
                                                  THEN hstore(substation.tags)->'operator' 
                                                  ELSE 'NA' END) as operator, 
		                            (CASE WHEN hstore(substation.tags)->'operator' in ('DB_Energie','DB Netz AG','DB Energie GmbH','DB Netz')
		                                  THEN 'see operator' 
		                                  ELSE 
                                                  (CASE WHEN '16.7' = ANY( string_to_array(hstore(substation.tags)->'frequency',';')) 
                                                             OR '16.67' = ANY( string_to_array(hstore(substation.tags)->'frequency',';')) 
		     		                        THEN 'see frequency' 
		     		                        ELSE 'no' 
				                        END)
		                                  END) as dbahn,
		                                  status
	                               FROM model_draft.substation_110kV substation ORDER BY osm_www;

                 ALTER VIEW model_draft.summary_total OWNER TO oeuser; ''')

create_view_summary_total()
write_into_tenth_file()
