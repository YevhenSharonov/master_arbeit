from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# HVMV Substation
# Abstract HVMV Substations of the high voltage level from OSM.
# This script abstracts substations of the high voltage level from openstreetmap data.
# All substations that are relevant transition points between the transmission and distribution grid are identified, irrelevant ones are disregarded.
#
# __copyright__   = "DLR Institute for Networked Energy Systems"
# __license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
# __url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
# __author__      = "lukasol, C. Matke, Ludee"


# hvmv substations, grant (oeuser) and comment on table, scenario log
def create_table_hvmv_substations(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP TABLE IF EXISTS model_draft.ego_grid_hvmv_substation CASCADE;
                         CREATE TABLE model_draft.ego_grid_hvmv_substation (
	                     subst_id   serial NOT NULL,
	                     lon        float NOT NULL,
	                     lat        float NOT NULL,
	                     point	   geometry(Point,4326) NOT NULL,
	                     polygon	   geometry NOT NULL,	
	                     voltage    text,
	                     power_type text,
	                     substation text,
	                     osm_id     text PRIMARY KEY,
	                     osm_www    text NOT NULL,
	                     frequency  text,
	                     subst_name text,
	                     ref        text,
	                     operator   text,
	                     dbahn      text,
	                     status	   smallint NOT NULL); 

                 ALTER TABLE model_draft.ego_grid_hvmv_substation OWNER TO oeuser;

                 COMMENT ON TABLE model_draft.ego_grid_hvmv_substation IS '{
                     "comment": "eGoDP - Temporary table",
                     "version": "v0.4.5" }';

                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','openstreetmap','osm_deu_ways','ego_dp_substation_hvmv.sql',' ');
                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','openstreetmap','osm_deu_polygon','ego_dp_substation_hvmv.sql',' ');
                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','openstreetmap','osm_deu_nodes','ego_dp_substation_hvmv.sql',' ');
                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','openstreetmap','osm_deu_line','ego_dp_substation_hvmv.sql',' '); ''')


# WAY: create view of way substations, grant (oeuser), scenario log 
def create_view_way_substation(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.way_substations CASCADE;
                         CREATE VIEW model_draft.way_substations 
                                  AS
	                             SELECT openstreetmap.osm_deu_ways.id, openstreetmap.osm_deu_ways.tags, openstreetmap.osm_deu_polygon.geom
	                               FROM openstreetmap.osm_deu_ways JOIN openstreetmap.osm_deu_polygon ON openstreetmap.osm_deu_ways.id = openstreetmap.osm_deu_polygon.osm_id
	                              WHERE hstore(openstreetmap.osm_deu_ways.tags)->'power' in ('substation','sub_station','station')
	                       UNION 
	                             SELECT openstreetmap.osm_deu_ways.id, openstreetmap.osm_deu_ways.tags, openstreetmap.osm_deu_line.geom
	                               FROM openstreetmap.osm_deu_ways JOIN openstreetmap.osm_deu_line ON openstreetmap.osm_deu_ways.id = openstreetmap.osm_deu_line.osm_id
	                              WHERE hstore(openstreetmap.osm_deu_ways.tags)->'power' in ('substation','sub_station','station'); 

                 ALTER VIEW model_draft.way_substations OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','way_substations','ego_dp_substation_hvmv.sql',' '); ''')


# WAY: create view of way substations with 110kV, grant (oeuser), scenario log 
def create_view_way_substation_110kv(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.way_substations_with_110kV CASCADE;
                         CREATE VIEW model_draft.way_substations_with_110kV 
                                  AS
	                             SELECT * 
	                               FROM model_draft.way_substations
	                              WHERE '110000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';')) 
		                            OR '60000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';'));

                 ALTER VIEW model_draft.way_substations_with_110kV OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','way_substations_with_110kV','ego_dp_substation_hvmv.sql',' '); ''')


#WAY: create view of substations without 110kV
def create_view_way_substation_without_110kv(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.way_substations_without_110kV CASCADE;
                         CREATE VIEW model_draft.way_substations_without_110kV 
                                  AS
                                     SELECT * 
                                       FROM model_draft.way_substations
	                              WHERE not '110000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';')) 
		                            AND not '60000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';')) 
		                            OR not exist(hstore(model_draft.way_substations.tags),'voltage');

                 ALTER VIEW model_draft.way_substations_without_110kV OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','way_substations_without_110kV','ego_dp_substation_hvmv.sql',' '); ''')


# NODE: create view of 110kV node substations, grant (oeuser), scenario log
def create_view_node_substation_110kv(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.node_substations_with_110kV CASCADE;
                         CREATE VIEW model_draft.node_substations_with_110kV 
                                  AS
	                             SELECT openstreetmap.osm_deu_nodes.id, openstreetmap.osm_deu_nodes.tags, openstreetmap.osm_deu_point.geom
	                               FROM openstreetmap.osm_deu_nodes JOIN openstreetmap.osm_deu_point ON openstreetmap.osm_deu_nodes.id = openstreetmap.osm_deu_point.osm_id
	                              WHERE '110000' = ANY( string_to_array(hstore(openstreetmap.osm_deu_nodes.tags)->'voltage',';')) 
		                            AND hstore(openstreetmap.osm_deu_nodes.tags)->'power' in ('substation','sub_station','station') 
		                            OR '60000' = ANY( string_to_array(hstore(openstreetmap.osm_deu_nodes.tags)->'voltage',';')) 
		                            AND hstore(openstreetmap.osm_deu_nodes.tags)->'power' in ('substation','sub_station','station');

                 ALTER VIEW model_draft.node_substations_with_110kV OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','node_substations_with_110kV','ego_dp_substation_hvmv.sql',' '); ''')


# LINES 110kV: create view of 110kV lines, grant (oeuser), scenario log
def create_view_lines_substation_110kv(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.way_lines_110kV CASCADE;
                         CREATE VIEW model_draft.way_lines_110kV 
                                  AS
	                             SELECT openstreetmap.osm_deu_ways.id, openstreetmap.osm_deu_ways.tags, openstreetmap.osm_deu_line.geom 
	                               FROM openstreetmap.osm_deu_ways JOIN openstreetmap.osm_deu_line ON openstreetmap.osm_deu_ways.id = openstreetmap.osm_deu_line.osm_id
	                              WHERE '110000' = ANY( string_to_array(hstore(openstreetmap.osm_deu_ways.tags)->'voltage',';')) 
		                            AND NOT hstore(openstreetmap.osm_deu_ways.tags)->'power' 
                                            IN ('minor_line','razed','dismantled:line','historic:line','construction','planned','proposed','abandoned:line','sub_station','abandoned','substation') 
		                            OR '60000' = ANY( string_to_array(hstore(openstreetmap.osm_deu_ways.tags)->'voltage',';')) 
		                            AND NOT hstore(openstreetmap.osm_deu_ways.tags)->'power' 
                                            IN ('minor_line','razed','dismantled:line','historic:line','construction','planned','proposed','abandoned:line','sub_station','abandoned','substation');

                 ALTER VIEW model_draft.way_lines_110kV OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','way_lines_110kV','ego_dp_substation_hvmv.sql',' '); ''')


# INTERSECTION: create view from substations without 110kV tag that contain 110kV line
# grant (oeuser), scenario log
def create_view_way_substations_without_110kv_intersected_by_110kv_line(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.way_substations_without_110kV_intersected_by_110kV_line CASCADE;
                         CREATE VIEW model_draft.way_substations_without_110kV_intersected_by_110kV_line 
                                  AS
	                             SELECT DISTINCT model_draft.way_substations_without_110kV.* 
	                               FROM model_draft.way_substations_without_110kV, model_draft.way_lines_110kV
	                              WHERE ST_Contains(model_draft.way_substations_without_110kV.geom,ST_StartPoint(model_draft.way_lines_110kV.geom)) 
		                            OR ST_Contains(model_draft.way_substations_without_110kV.geom,ST_EndPoint(model_draft.way_lines_110kV.geom));

                 ALTER VIEW model_draft.way_substations_without_110kV_intersected_by_110kV_line OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','way_substations_without_110kV_intersected_by_110kV_line','ego_dp_substation_hvmv.sql',' '); ''')


# drop view substation_100kV and create new view substation_100kV, grant (oeuser), scenario log
def drop_create_view_substation_110kv(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.substation_110kV CASCADE;
                         CREATE VIEW model_draft.substation_110kV 
                                  AS
	                             SELECT *,
		                            'http://www.osm.org/way/'|| model_draft.way_substations_with_110kV.id as osm_www,
		                            'w'|| model_draft.way_substations_with_110kV.id as osm_id,
		                            '1'::smallint as status
	                               FROM model_draft.way_substations_with_110kV
	                              UNION 
	                             SELECT *,
		                            'http://www.osm.org/way/'|| model_draft.way_substations_without_110kV_intersected_by_110kV_line.id as osm_www,
		                            'w'|| model_draft.way_substations_without_110kV_intersected_by_110kV_line.id as osm_id,
		                            '2'::smallint as status
	                               FROM model_draft.way_substations_without_110kV_intersected_by_110kV_line
	                              UNION 
	                             SELECT *,
		                            'http://www.osm.org/node/'|| model_draft.node_substations_with_110kV.id as osm_www,
		                            'n'|| model_draft.node_substations_with_110kV.id as osm_id,
		                            '3'::smallint as status
	                               FROM model_draft.node_substations_with_110kV;

                 ALTER VIEW model_draft.substation_110kV OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','substation_110kV','ego_dp_substation_hvmv.sql',' '); ''')


# create view summary_total that contains substations without any filter, grant (oeuser), scenario log
def create_view_summary_total(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
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
		                            CASE WHEN hstore(substation.tags)->'substation' <> '' 
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

                 ALTER VIEW model_draft.summary_total OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','summary_total','ego_dp_substation_hvmv.sql',' '); ''')


# create view that filters irrelevant tags, index gist (geom), grant (oeuser), scenario log
def create_view_filters_irrelevant_tags(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.summary CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.summary 
                                               AS
	                                          SELECT *
	                                            FROM model_draft.summary_total
	                                           WHERE dbahn = 'no' AND substation NOT IN ('traction','transition');

                 CREATE INDEX summary_gix ON model_draft.summary USING GIST (polygon);

                 ALTER MATERIALIZED VIEW model_draft.summary OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','summary','ego_dp_substation_hvmv.sql',' '); ''')


# eliminate substation that are not within VG250, grant (oeuser), scenario log 
def create_view_substation_not_within_vg250(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.summary_de CASCADE;
                         CREATE VIEW model_draft.summary_de 
                                  AS
	                             SELECT *
	                               FROM model_draft.summary, boundaries.bkg_vg250_1_sta_union_mview as vg
	                              WHERE ST_Transform(vg.geom,4326) && model_draft.summary.polygon 
	                                    AND ST_CONTAINS(ST_Transform(vg.geom,4326),model_draft.summary.polygon);

                 ALTER VIEW model_draft.summary_de OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','boundaries','bkg_vg250_1_sta_union_mview','ego_dp_substation_hvmv.sql',' ');
                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','summary_de','ego_dp_substation_hvmv.sql',' '); ''')


# create view with buffer of 75m around polygons, grant (oeuser), scenario log
def create_view_buffer_75m_around_polygons(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75 CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.buffer_75 
                                               AS
	                                          SELECT osm_id, ST_Area(ST_Transform(model_draft.summary_de.polygon,4326)) as area, 
                                                         ST_Buffer_Meters(ST_Transform(model_draft.summary_de.polygon,4326), 75) as buffer_75
	                                            FROM model_draft.summary_de;

                 ALTER MATERIALIZED VIEW model_draft.buffer_75 OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','buffer_75','ego_dp_substation_hvmv.sql',' '); ''')


# create second view with same data to compare, grant (oeuser), scenario log
def create_second_view_with_same_data(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75_a CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.buffer_75_a 
                                               AS
	                                          SELECT osm_id, ST_Area(ST_Transform(model_draft.summary_de.polygon,4326)) as area_a, 
                                                         ST_Buffer_Meters(ST_Transform(model_draft.summary_de.polygon,4326), 75) as buffer_75_a
	                                            FROM model_draft.summary_de;

                 ALTER MATERIALIZED VIEW model_draft.buffer_75_a OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','buffer_75_a','ego_dp_substation_hvmv.sql',' '); ''')


# create view to eliminate smaller substations where buffers intersect, grant (oeuser), scenario log
def create_view_eiliminate_smaller_substations(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.substations_to_drop CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.substations_to_drop 
                                               AS
	                                          SELECT DISTINCT
	                                                 (CASE WHEN model_draft.buffer_75.area < model_draft.buffer_75_a.area_a 
                                                               THEN model_draft.buffer_75.osm_id 
                                                               ELSE model_draft.buffer_75_a.osm_id END) as osm_id,
	                                                 (CASE WHEN model_draft.buffer_75.area < model_draft.buffer_75_a.area_a 
                                                               THEN model_draft.buffer_75.area 
                                                               ELSE model_draft.buffer_75_a.area_a END) as area,
	                                                 (CASE WHEN model_draft.buffer_75.area < model_draft.buffer_75_a.area_a 
                                                               THEN model_draft.buffer_75.buffer_75 
                                                               ELSE model_draft.buffer_75_a.buffer_75_a END) as buffer
	                                            FROM model_draft.buffer_75, model_draft.buffer_75_a
	                                           WHERE ST_Intersects(model_draft.buffer_75.buffer_75, model_draft.buffer_75_a.buffer_75_a)
	                                                 AND NOT model_draft.buffer_75.osm_id = model_draft.buffer_75_a.osm_id;
     
                 ALTER MATERIALIZED VIEW model_draft.substations_to_drop OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','substations_to_drop','ego_dp_substation_hvmv.sql',' '); ''')


# filter those substations and create final_result, grant (oeuser), scenario log
def create_view_filter_substations_final_result(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.final_result CASCADE;
                         CREATE VIEW model_draft.final_result 
                                  AS
	                             SELECT * 
	                             FROM model_draft.summary_de
	                             WHERE model_draft.summary_de.osm_id 
                                           NOT IN ( SELECT model_draft.substations_to_drop.osm_id FROM model_draft.substations_to_drop);

                 ALTER VIEW model_draft.final_result OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','final_result','ego_dp_substation_hvmv.sql',' '); ''')


# insert results, scenario log
def insert_results(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 INSERT INTO model_draft.ego_grid_hvmv_substation (lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status)
	              SELECT lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status
	                FROM model_draft.final_result;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','output','model_draft','ego_grid_hvmv_substation','ego_dp_substation_hvmv.sql',' ');  ''')


# drop
def drop(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                              DROP VIEW IF EXISTS model_draft.final_result CASCADE;
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.substations_to_drop CASCADE;
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75 CASCADE;
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75_a CASCADE;
                              DROP VIEW IF EXISTS model_draft.summary_de CASCADE;
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.summary CASCADE;
                              DROP VIEW IF EXISTS model_draft.summary_total CASCADE;
                              DROP VIEW IF EXISTS model_draft.substation_110kV CASCADE;
                              DROP VIEW IF EXISTS model_draft.way_substations_without_110kV_intersected_by_110kV_line CASCADE;
                              DROP VIEW IF EXISTS model_draft.way_lines_110kV CASCADE;
                              DROP VIEW IF EXISTS model_draft.node_substations_with_110kV CASCADE;
                              DROP VIEW IF EXISTS model_draft.way_substations_without_110kV CASCADE;
                              DROP VIEW IF EXISTS model_draft.way_substations_with_110kV CASCADE;
                              DROP VIEW IF EXISTS model_draft.way_substations CASCADE; ''')


dag_params = {
    'dag_id': 'ego_dp_substation_hvmv',
    'start_date': datetime(2020, 7, 7),
    'schedule_interval': None
}


with DAG(**dag_params) as dag:

    create_table_hvmv_substations = PythonOperator(
        task_id='create_table_hvmv_substations',
        python_callable=create_table_hvmv_substations
    )

    create_view_way_substation = PythonOperator(
        task_id='create_view_way_substation',
        python_callable=create_view_way_substation
    )

    create_view_way_substation_110kv = PythonOperator(
        task_id='create_view_way_substation_110kv',
        python_callable=create_view_way_substation_110kv
    )

    create_view_way_substation_without_110kv = PythonOperator(
        task_id='create_view_way_substation_without_110kv',
        python_callable=create_view_way_substation_without_110kv
    )

    create_view_node_substation_110kv = PythonOperator(
        task_id='create_view_node_substation_110kv',
        python_callable=create_view_node_substation_110kv
    )

    create_view_lines_substation_110kv = PythonOperator(
        task_id='create_view_lines_substation_110kv',
        python_callable=create_view_lines_substation_110kv
    )

    create_view_way_substations_without_110kv_intersected_by_110kv_line = PythonOperator(
        task_id='create_view_way_substations_without_110kv_intersected_by_110kv_line',
        python_callable=create_view_way_substations_without_110kv_intersected_by_110kv_line
    )

    drop_create_view_substation_110kv = PythonOperator(
        task_id='drop_create_view_substation_110kv',
        python_callable=drop_create_view_substation_110kv
    )

    create_view_summary_total = PythonOperator(
        task_id='create_view_summary_total',
        python_callable=create_view_summary_total
    )

    create_view_filters_irrelevant_tags = PythonOperator(
        task_id='create_view_filters_irrelevant_tags',
        python_callable=create_view_filters_irrelevant_tags
    )

    create_view_substation_not_within_vg250 = PythonOperator(
        task_id='create_view_substation_not_within_vg250',
        python_callable=create_view_substation_not_within_vg250
    )

    create_view_buffer_75m_around_polygons = PythonOperator(
        task_id='create_view_buffer_75m_around_polygons',
        python_callable=create_view_buffer_75m_around_polygons
    )

    create_second_view_with_same_data = PythonOperator(
        task_id='create_second_view_with_same_data',
        python_callable=create_second_view_with_same_data
    )

    create_view_eiliminate_smaller_substations = PythonOperator(
        task_id='create_view_eiliminate_smaller_substations',
        python_callable=create_view_eiliminate_smaller_substations
    )

    create_view_filter_substations_final_result = PythonOperator(
        task_id='create_view_filter_substations_final_result',
        python_callable=create_view_filter_substations_final_result
    )

    insert_results = PythonOperator(
        task_id='insert_results',
        python_callable=insert_results
    )

    drop = PythonOperator(
        task_id='drop',
        python_callable=drop
    )

    trigger_next_dag =  TriggerDagRunOperator(
        trigger_dag_id='ego_dp_substation_ehv', 
        task_id='create_table_ego_grid_ehv_substation'
    )


    
    create_table_hvmv_substations >> create_view_way_substation >> create_view_way_substation_110kv >> create_view_way_substation_without_110kv >> create_view_node_substation_110kv >> create_view_lines_substation_110kv >> create_view_way_substations_without_110kv_intersected_by_110kv_line >> drop_create_view_substation_110kv >> create_view_summary_total >> create_view_filters_irrelevant_tags >> create_view_substation_not_within_vg250 >> create_view_buffer_75m_around_polygons >> create_second_view_with_same_data >> create_view_eiliminate_smaller_substations >> create_view_filter_substations_final_result >> insert_results >> drop >> trigger_next_dag

