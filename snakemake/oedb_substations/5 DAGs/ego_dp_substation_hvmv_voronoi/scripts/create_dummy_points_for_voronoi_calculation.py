from sqlalchemy import create_engine
import datetime
import time

def write_into_fourth_file():

    f = open("/home/sharonov/snakemake/third.txt")
    f1 = open("/home/sharonov/snakemake/fourth.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask create_dummy_points_for_voronoi_calculation:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask create_dummy_points_for_voronoi_calculation:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def create_dummy_points_for_voronoi_calculation():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                DROP TABLE IF EXISTS model_draft.ego_grid_hvmv_substation_dummy CASCADE;
                         CREATE TABLE model_draft.ego_grid_hvmv_substation_dummy (
	                     subst_id integer,
	                     subst_name text,
	                     geom geometry(Point,3035),
	                     CONSTRAINT ego_grid_hvmv_substation_dummy_pkey PRIMARY KEY (subst_id));

                 INSERT INTO model_draft.ego_grid_hvmv_substation_dummy (subst_id,subst_name,geom)
                      SELECT '9001' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(3878765.05927874 3342870.31889155)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9002' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(3941166.84473566 3530017.71307212)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9003' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4033601.91782991 3691498.91137808)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9004' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4152362.22315766 3789791.51576683)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9005' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4329393.62365717 3830200.38704261)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9006' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4495844.04212254 3826709.11082927)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9007' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4645016.94115968 3729362.79329962)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9008' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4774984.93447502 3477163.19376104)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9009' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4843018.75954041 3201846.85332402)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9010' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4883145.96311377 2901213.21783621)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9011' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4847739.56682014 2666951.87655141)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9012' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4686275.85532184 2472337.14136933)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9013' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4486219.50548571 2392980.74648555)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9014' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4263444.02272107 2368483.9483323)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9015' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(4039063.04090959 2439583.67779797)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9016' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(3904569.75117475 2609736.50599677)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9017' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(3852322.49802 2849800.43518705)'),3035) ::geometry(Point,3035) AS geom UNION ALL 
                      SELECT '9018' ::integer 
                          AS subst_id, 'DUMMY' AS subst_name, ST_SetSRID(ST_GeomFromText('POINT(3843258.70046489 3109607.27281094)'),3035) ::geometry(Point,3035) AS geom;

                 CREATE INDEX substations_dummy_geom_idx
	                      ON model_draft.ego_grid_hvmv_substation_dummy USING gist (geom);

                 ALTER TABLE model_draft.ego_grid_hvmv_substation_dummy OWNER TO oeuser;

                 COMMENT ON TABLE model_draft.ego_grid_hvmv_substation_dummy IS '{
                                  "comment": "eGoDP - Temporary table",
                                  "version": "v0.4.5" }' ;

                 SELECT obj_description('model_draft.ego_grid_hvmv_substation_dummy' ::regclass) ::json; ''')

create_dummy_points_for_voronoi_calculation()
write_into_fourth_file()
