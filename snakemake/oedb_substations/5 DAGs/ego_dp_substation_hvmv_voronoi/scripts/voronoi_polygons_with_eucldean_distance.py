from sqlalchemy import create_engine
import datetime
import time

def write_into_log_file():

    f = open("/home/sharonov/snakemake/fourth.txt")
    f1 = open("/home/sharonov/snakemake/log_file.txt", "a")
    for x in f.readlines():
        f1.write(x)
    now = datetime.datetime.now()
    if execution_time <= 1:
        f1.write("\nTask voronoi_polygons_with_eucldean_distance:" + "\n    execution date: " + str(now) + "\n    execution time: {:>.3f}".format(execution_time) + " seconds" + "\n")
    else:
        ty_res = time.gmtime(execution_time)
        res = time.strftime("%H:%M:%S",ty_res)
        f1.write("\nTask voronoi_polygons_with_eucldean_distance:" + "\n    execution date: " + str(now) + "\n    execution time: " + res + "\n")
    f.close()
    f1.close()

def voronoi_polygons_with_eucldean_distance():

    global execution_time
    start_time = time.monotonic()

    engine = create_engine('postgresql://postgres:12345678@localhost:5432/oedb')

    with engine.connect() as connection:
        result = connection.execute('''
                 DROP TABLE IF EXISTS model_draft.ego_grid_hvmv_substation_voronoi CASCADE;    -- name 1/2
	                         WITH    -- sample set of points to work with
		            Sample AS 
                                      (SELECT ST_SetSRID(ST_Union(ST_Collect(a.geom,b.geom)), 0) AS geom
		                      FROM model_draft.ego_grid_hvmv_substation AS a,
		     	                   model_draft.ego_grid_hvmv_substation_dummy AS b),    -- input points
		                      -- Build edges and circumscribe points to generate centroids
		             Edges AS 
                                      (SELECT id,
			              UNNEST(ARRAY['e1','e2','e3']) EdgeName,
			              UNNEST(ARRAY[
				                 ST_MakeLine(p1,p2) ,
				                 ST_MakeLine(p2,p3) ,
				                 ST_MakeLine(p3,p1)]) Edge,
			              ST_Centroid(ST_ConvexHull(ST_Union(-- Done this way due to issues I had with LineToCurve
				                 ST_CurveToLine(REPLACE(ST_AsText(ST_LineMerge(ST_Union(ST_MakeLine(p1,p2),ST_MakeLine(p2,p3)))),'LINE','CIRCULAR'),15),
				                 ST_CurveToLine(REPLACE(ST_AsText(ST_LineMerge(ST_Union(ST_MakeLine(p2,p3),ST_MakeLine(p3,p1)))),'LINE','CIRCULAR'),15) ))) ct
		                      FROM ( 	
		                      -- Decompose to points
			              SELECT id,
				             ST_PointN(g,1) p1,
				             ST_PointN(g,2) p2,
				             ST_PointN(g,3) p3
			              FROM (SELECT (gd).Path id, ST_ExteriorRing((gd).geom) g    -- ID andmake triangle a linestring
				           FROM (SELECT (ST_Dump(ST_DelaunayTriangles(geom))) gd 
                                                FROM Sample) a -- Get Delaunay Triangles
				           )b
		                      ) c )
                               SELECT ST_SetSRID((ST_Dump(ST_Polygonize(ST_Node(ST_LineMerge(ST_Union(v, (SELECT ST_ExteriorRing(ST_ConvexHull(ST_Union(ST_Union(ST_Buffer(edge,20),ct)))) 
                                 FROM Edges))))))).geom, 2180) geom
	                         INTO model_draft.ego_grid_hvmv_substation_voronoi    -- name 2/2
                                      FROM (SELECT    -- Create voronoi edges and reduce to a multilinestring
			                           ST_LineMerge(ST_Union(ST_MakeLine(
			                           x.ct,
			                           CASE
			                           WHEN y.id IS NULL 
                                                   THEN
			                           CASE WHEN ST_Within(x.ct,
			                                     (SELECT ST_ConvexHull(geom) FROM sample)) 
                                                        THEN    -- Don't draw lines back towards the original set
			                                -- Project line out twice the distance from convex hull
			                                     ST_MakePoint(ST_X(x.ct) + ((ST_X(ST_Centroid(x.edge)) - ST_X(x.ct)) * 200),ST_Y(x.ct) + ((ST_Y(ST_Centroid(x.edge)) - ST_Y(x.ct)) * 200))
			                                END
			                           ELSE y.ct
			                           END ))) v
		                      FROM Edges x
		                      LEFT OUTER JOIN    -- Self Join based on edges
		                                      Edges y ON x.id <> y.id AND ST_Equals(x.edge,y.edge)
                                      ) z;

                 ALTER TABLE model_draft.ego_grid_hvmv_substation_voronoi
	           ADD COLUMN id serial,
	           ADD COLUMN subst_id integer,
	           ADD COLUMN subst_sum integer,
	           ADD PRIMARY KEY (id),
	         ALTER COLUMN geom TYPE geometry(POLYGON,3035) USING ST_SETSRID(geom,3035);

                 CREATE INDEX ego_grid_hvmv_substation_voronoi_geom_idx
	                   ON model_draft.ego_grid_hvmv_substation_voronoi 
                        USING gist (geom);

                 ALTER TABLE model_draft.ego_grid_hvmv_substation_voronoi OWNER TO oeuser;

                 /* -- delete dummy points from substations and voronoi (18 Points)
                 DELETE FROM model_draft.ego_grid_hvmv_substation WHERE subst_name='DUMMY';
                 DELETE FROM model_draft.ego_grid_hvmv_substation_voronoi WHERE subst_id IS NULL; */

                 COMMENT ON TABLE model_draft.ego_grid_hvmv_substation_voronoi IS '{
	                          "comment": "eGoDP - Temporary table",
	                          "version": "v0.4.5" }' ;

                 SELECT obj_description('model_draft.ego_grid_hvmv_substation_voronoi' ::regclass) ::json; ''')

voronoi_polygons_with_eucldean_distance()
write_into_log_file()
