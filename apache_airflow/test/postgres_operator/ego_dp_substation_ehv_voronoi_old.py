from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

# EHV Substation Voronoi
# Voronoi polygons with eucldean distance on EHV Substation.
# Manhattan distance would be better but not available in sql.
# 
# __copyright__   = "Flensburg University of Applied Sciences, Centre for Sustainable Energy Systems"
# __license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
# __url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
# __author__      = "IlkaCu, Ludee"
#
#
# VORONOI with  220 and 380 kV substations


dag_params = {
    'dag_id': 'ego_dp_substation_ehv_voronoi',
    'start_date': datetime(2020, 7, 7),
    'schedule_interval': None
}


with DAG(**dag_params) as dag:

# add dummy points
    add_dummy_points = PostgresOperator(
        task_id='add_dummy_points',
        postgres_conn_id='postgres_oedb',
        sql='''
            INSERT INTO model_draft.ego_grid_ehv_substation (subst_name, point, subst_id, otg_id, lon, lat, polygon, osm_id, osm_www, status)
                 SELECT 'DUMMY', ST_TRANSFORM(geom,4326), subst_id, subst_id, ST_X (ST_Transform (geom, 4326)), ST_Y (ST_Transform (geom, 4326)), 
                        'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', 'dummy'||row_number() OVER(), 'dummy', 0
                   FROM model_draft.ego_grid_hvmv_substation_dummy; ''',
    )

# Execute voronoi algorithm with 220 and 380 kV substations, "Create Index GIST (geom)"   (OK!) 11.000ms =0,
# "Set id and SRID"   (OK!) -> 100ms =0, Delete Dummy-points from ehv_substations and ehv_voronoi 
# set unique constraint on subst_id, Set PK and FK, metadata
    execute_voronoi_algorithm_with_220_and_380kv_substations = PostgresOperator(
        task_id='execute_voronoi_algorithm_with_220_and_380kv_substations',
        postgres_conn_id='postgres_oedb',
        sql='''
            DROP TABLE IF EXISTS model_draft.ego_grid_ehv_substation_voronoi CASCADE;
                            WITH 
                            -- Sample set of points to work with
                       Sample AS 
                                 (SELECT   ST_SetSRID(ST_Union(pts.point), 0) AS geom
		                 FROM model_draft.ego_grid_ehv_substation AS pts
		                 /*WHERE pts.otg_id IS NOT NULL*/),    -- INPUT 1/2, this only includes substations with an otg_id
                       -- Build edges and circumscribe points to generate a centroid
                        Edges AS 
                                 (SELECT id,
                                 UNNEST(ARRAY['e1','e2','e3']) EdgeName,
                                 UNNEST(ARRAY[
                                            ST_MakeLine(p1,p2) ,
                                            ST_MakeLine(p2,p3) ,
                                            ST_MakeLine(p3,p1)]) Edge,
                                 ST_Centroid(ST_ConvexHull(ST_Union(    -- Done this way due to issues I had with LineToCurve
                                            ST_CurveToLine(REPLACE(ST_AsText(ST_LineMerge(ST_Union(ST_MakeLine(p1,p2),ST_MakeLine(p2,p3)))),'LINE','CIRCULAR'),15),
                                            ST_CurveToLine(REPLACE(ST_AsText(ST_LineMerge(ST_Union(ST_MakeLine(p2,p3),ST_MakeLine(p3,p1)))),'LINE','CIRCULAR'),15)
                                 ))) ct      
                                 FROM (
                                 -- Decompose to points
                                      SELECT id,
                                             ST_PointN(g,1) p1,
                                             ST_PointN(g,2) p2,
                                             ST_PointN(g,3) p3
                                      FROM (SELECT (gd).Path id, ST_ExteriorRing((gd).geom) g    -- ID andmake triangle a linestring
                                           FROM (SELECT (ST_Dump(ST_DelaunayTriangles(geom))) gd FROM Sample) a    -- Get Delaunay Triangles
                                      )b
                                 )c
                                 )
                          SELECT ST_SetSRID((ST_Dump(ST_Polygonize(ST_Node(ST_LineMerge(ST_Union(v, (SELECT ST_ExteriorRing(ST_ConvexHull(ST_Union(ST_Union(ST_Buffer(edge,20),ct)))) 
                            FROM Edges))))))).geom, 2180) geom
                            INTO model_draft.ego_grid_ehv_substation_voronoi    -- INPUT 2/2
                            FROM (SELECT    -- Create voronoi edges and reduce to a multilinestring
                                         ST_LineMerge(ST_Union(ST_MakeLine(
                                         x.ct,
                                         CASE 
                                         WHEN y.id IS NULL 
                                         THEN
                                         CASE WHEN ST_Within(
                                                   x.ct,
                                                   (SELECT ST_ConvexHull(geom) 
                                                   FROM sample)) 
                                              THEN    -- Don't draw lines back towards the original set
                                              -- Project line out twice the distance from convex hull
                                                   ST_MakePoint(ST_X(x.ct) + ((ST_X(ST_Centroid(x.edge)) - ST_X(x.ct)) * 200),ST_Y(x.ct) + ((ST_Y(ST_Centroid(x.edge)) - ST_Y(x.ct)) * 200))
                                              END
                                              ELSE 
                                                   y.ct
                                              END
                                              ))) v
                                 FROM Edges x 
                                 LEFT OUTER JOIN    -- Self Join based on edges
                                                 Edges y ON x.id <> y.id AND ST_Equals(x.edge,y.edge)
                                 ) z;

            CREATE INDEX voronoi_ehv_geom_idx
	              ON model_draft.ego_grid_ehv_substation_voronoi
	           USING GIST (geom);

            ALTER TABLE model_draft.ego_grid_ehv_substation_voronoi
	      ADD COLUMN subst_id integer,
	    ALTER COLUMN geom TYPE geometry(POLYGON,4326) USING ST_SETSRID(geom,4326);

            UPDATE model_draft.ego_grid_ehv_substation_voronoi a
	       SET subst_id = b.subst_id
	      FROM model_draft.ego_grid_ehv_substation b
	     WHERE ST_Intersects(a.geom, b.point) =TRUE; 

            DELETE FROM model_draft.ego_grid_ehv_substation_voronoi 
	          WHERE subst_id IN (SELECT subst_id FROM model_draft.ego_grid_ehv_substation WHERE subst_name = 'DUMMY');

            DELETE FROM model_draft.ego_grid_ehv_substation 
                  WHERE subst_name='DUMMY';

            ALTER TABLE model_draft.ego_grid_ehv_substation DROP CONSTRAINT IF EXISTS unique_substation;
            ALTER TABLE model_draft.ego_grid_ehv_substation
              ADD CONSTRAINT unique_substation UNIQUE (subst_id);

            ALTER TABLE model_draft.ego_grid_ehv_substation_voronoi 
	      ADD CONSTRAINT subst_fk FOREIGN KEY (subst_id) REFERENCES model_draft.ego_grid_ehv_substation (subst_id),
	      ADD PRIMARY KEY (subst_id);

            ALTER TABLE model_draft.ego_grid_ehv_substation_voronoi OWNER TO oeuser;

            COMMENT ON TABLE model_draft.ego_grid_ehv_substation_voronoi IS '{
	                     "comment": "eGoDP - Temporary table",
	                     "version": "v0.4.2" }' ; ''',
    )


    add_dummy_points >> execute_voronoi_algorithm_with_220_and_380kv_substations
