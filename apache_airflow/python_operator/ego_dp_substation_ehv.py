from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# EHV Substation
# Abstract EHV Substations of the extra high voltage level from OSM.
# This script abstracts substations of the extra high voltage level from openstreetmap data.
#
# __copyright__   = "DLR Institute for Networked Energy Systems"
# __license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
# __url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
# __author__      = "lukasol, C. Matke, Ludee"


# ehv substation drop/create table, grant (oeuser), metadata, scenario log
def create_table_ego_grid_ehv_substation(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP TABLE IF EXISTS model_draft.ego_grid_ehv_substation CASCADE;
                         CREATE TABLE model_draft.ego_grid_ehv_substation (
	                     subst_id   serial NOT NULL,
	                     lon        float NOT NULL,
	                     lat        float NOT NULL,
	                     point	   geometry(Point,4326) NOT NULL,
	                     polygon	   geometry NOT NULL,	
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
	             "version": "v0.4.5" }' ;
     
                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','openstreetmap','osm_deu_ways','ego_dp_substation_ehv.sql',' ');
                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','openstreetmap','osm_deu_polygon','ego_dp_substation_ehv.sql',' ');
                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','openstreetmap','osm_deu_line','ego_dp_substation_ehv.sql',' ');
                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','openstreetmap','osm_deu_nodes','ego_dp_substation_ehv.sql',' ');
                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','openstreetmap','osm_deu_rels','ego_dp_substation_ehv.sql',' ');   ''')


# create view from OSM way substations, grant (oeuser), scenario log
def create_view_way_substations(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.way_substations CASCADE;
                         CREATE VIEW model_draft.way_substations 
                                  AS
	                             SELECT openstreetmap.osm_deu_ways.id, openstreetmap.osm_deu_ways.tags, openstreetmap.osm_deu_polygon.geom
	                               FROM openstreetmap.osm_deu_ways JOIN openstreetmap.osm_deu_polygon 
                                            ON openstreetmap.osm_deu_ways.id = openstreetmap.osm_deu_polygon.osm_id
	                              WHERE hstore(openstreetmap.osm_deu_ways.tags)->'power' in ('substation','sub_station','station')
	                              UNION
	                             SELECT openstreetmap.osm_deu_ways.id, openstreetmap.osm_deu_ways.tags, openstreetmap.osm_deu_line.geom
	                               FROM openstreetmap.osm_deu_ways JOIN openstreetmap.osm_deu_line 
                                            ON openstreetmap.osm_deu_ways.id = openstreetmap.osm_deu_line.osm_id
	                              WHERE hstore(openstreetmap.osm_deu_ways.tags)->'power' in ('substation','sub_station','station');

                 ALTER VIEW model_draft.way_substations OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','way_substations','ego_dp_substation_ehv.sql',' '); ''')


# create view way substations with hoes, grant (oeuser), scenario log
def create_view_way_substations_with_hoes(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.way_substations_with_hoes CASCADE;
                         CREATE VIEW model_draft.way_substations_with_hoes 
                                  AS
	                             SELECT * 
	                               FROM model_draft.way_substations
	                              WHERE '220000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';')) 
		                            OR '380000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';'));

                 ALTER VIEW model_draft.way_substations_with_hoes OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','way_substations_with_hoes','ego_dp_substation_ehv.sql',' ');  ''')


# create view from OSM node substations, grant (oeuser), scenario log
def create_view_node_substations_with_hoes(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.node_substations_with_hoes CASCADE;
                         CREATE VIEW model_draft.node_substations_with_hoes 
                                  AS
	                             SELECT openstreetmap.osm_deu_nodes.id, openstreetmap.osm_deu_nodes.tags, openstreetmap.osm_deu_point.geom
	                               FROM openstreetmap.osm_deu_nodes JOIN openstreetmap.osm_deu_point 
                                            ON openstreetmap.osm_deu_nodes.id = openstreetmap.osm_deu_point.osm_id
	                              WHERE '220000' = ANY( string_to_array(hstore(openstreetmap.osm_deu_nodes.tags)->'voltage',';')) 
                                            AND hstore(openstreetmap.osm_deu_nodes.tags)->'power' in ('substation','sub_station','station') 
                                            OR '380000' = ANY( string_to_array(hstore(openstreetmap.osm_deu_nodes.tags)->'voltage',';')) 
                                            AND hstore(openstreetmap.osm_deu_nodes.tags)->'power' in ('substation','sub_station','station');

                 ALTER VIEW model_draft.node_substations_with_hoes OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','node_substations_with_hoes','ego_dp_substation_ehv.sql',' '); ''')


# RELATION: Erzeuge einen VIEW aus OSM relation substations
# Da die relations keine geometry besitzen wird mit folgender funktion der geometrische mittelpunkt der relation ermittelt,
# wobei hier der Mittelpunkt aus dem Rechteck ermittelt wird welches entsteht, wenn man die äußersten Korrdinaten für
# longitude und latitude wählt
# needs st_relation_geometry
def create_view_relation_substations_with_hoes(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.relation_substations_with_hoes CASCADE;
                         CREATE VIEW model_draft.relation_substations_with_hoes 
                                  AS
	                             SELECT openstreetmap.osm_deu_rels.id, openstreetmap.osm_deu_rels.tags, relation_geometry(openstreetmap.osm_deu_rels.members) as way
	                               FROM openstreetmap.osm_deu_rels 
	                              WHERE '220000' = ANY( string_to_array(hstore(openstreetmap.osm_deu_rels.tags)->'voltage',';')) 
		                            AND hstore(openstreetmap.osm_deu_rels.tags)->'power' in ('substation','sub_station','station') 
		                            OR '380000' = ANY( string_to_array(hstore(openstreetmap.osm_deu_rels.tags)->'voltage',';')) 
		                            AND hstore(openstreetmap.osm_deu_rels.tags)->'power' in ('substation','sub_station','station');

                 ALTER VIEW model_draft.relation_substations_with_hoes OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','relation_substations_with_hoes','ego_dp_substation_ehv.sql',' '); ''')


# create view substation_hoes, grant (oeuser), scenario log
def create_view_substation_hoes(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.substation_hoes CASCADE;
                         CREATE VIEW model_draft.substation_hoes 
                                  AS
	                             SELECT *,
		                            'http://www.osm.org/relation/'|| model_draft.relation_substations_with_hoes.id as osm_www,
		                            'r'|| model_draft.relation_substations_with_hoes.id as osm_id,
		                            '1'::smallint as status
	                               FROM model_draft.relation_substations_with_hoes
	                              UNION
	                             SELECT *,
		                            'http://www.osm.org/way/'|| model_draft.way_substations_with_hoes.id as osm_www,
		                            'w'|| model_draft.way_substations_with_hoes.id as osm_id,
		                            '2'::smallint as status
	                               FROM model_draft.way_substations_with_hoes
	                              UNION 
	                             SELECT *,
		                            'http://www.osm.org/node/'|| model_draft.node_substations_with_hoes.id as osm_www,
		                            'n'|| model_draft.node_substations_with_hoes.id as osm_id,
		                            '4'::smallint as status
	                               FROM model_draft.node_substations_with_hoes;

                 ALTER VIEW model_draft.substation_hoes OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','substation_hoes','ego_dp_substation_ehv.sql',' '); ''')


# create view summary_total_hoes that contains substations without any filter,
# grant (oeuser), scenario log
def create_view_summary_total_hoes(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.summary_total_hoes CASCADE;
                         CREATE VIEW model_draft.summary_total_hoes 
                                  AS
	                             SELECT ST_X(ST_Centroid(ST_Transform(substation.way,4326))) as lon,
		                            ST_Y(ST_Centroid(ST_Transform(substation.way,4326))) as lat,
		                            ST_Centroid(ST_Transform(substation.way,4326)) as point,
		                            ST_Transform(substation.way,4326) as polygon,
		                            (CASE WHEN hstore(substation.tags)->'voltage' <> '' 
                                                  THEN hstore(substation.tags)->'voltage' 
                                                  ELSE 'hoes' END) as voltage, 
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
		                                  ELSE (CASE WHEN '16.7' = ANY( string_to_array(hstore(substation.tags)->'frequency',';')) 
                                                                  OR '16.67' = ANY( string_to_array(hstore(substation.tags)->'frequency',';')) 
                                                             THEN 'see frequency' 
                                                             ELSE 'no' 
                                                             END)
                                                  END) as dbahn,
                                            status
	                               FROM model_draft.substation_hoes substation ORDER BY osm_www;

                 ALTER VIEW model_draft.summary_total_hoes OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','summary_total_hoes','ego_dp_substation_ehv.sql',' '); ''')


# create view that filters irrelevant tags, grant (oeuser)
def create_view_summary_hoes(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.summary_hoes CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.summary_hoes 
                                               AS
	                                          SELECT *
	                                            FROM model_draft.summary_total_hoes
	                                           WHERE dbahn = 'no' AND substation NOT IN ('traction','transition');

                 ALTER MATERIALIZED VIEW model_draft.summary_hoes OWNER TO oeuser; ''')


# create index gist, scenario log
def create_index_gist(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 CREATE INDEX summary_hoes_gix ON model_draft.summary_hoes USING GIST (polygon);

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','summary_hoes','ego_dp_substation_ehv.sql',' '); ''')


# create view for eliminating substation that are not within VG250, grant (oeuser), scenario log
def create_view_summary_de_hoes(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.summary_de_hoes CASCADE;
                         CREATE VIEW model_draft.summary_de_hoes 
                                  AS
	                             SELECT *
	                               FROM model_draft.summary_hoes, boundaries.bkg_vg250_1_sta_union_mview as vg
	                              WHERE ST_Transform(vg.geom,4326) && model_draft.summary_hoes.polygon 
	                                    AND ST_CONTAINS(ST_Transform(vg.geom,4326),model_draft.summary_hoes.polygon);

                 ALTER VIEW model_draft.summary_de_hoes OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','input','boundaries','bkg_vg250_1_sta_union_mview','ego_dp_substation_ehv.sql',' ');
                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','summary_de_hoes','ego_dp_substation_ehv.sql',' '); ''')


# create view with buffer of 75m around polygons, grant (oeuser), scenario log
def create_view_buffer_75_hoes(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75_hoes CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.buffer_75_hoes 
                                               AS
	                                          SELECT osm_id, ST_Area(ST_Transform(model_draft.summary_de_hoes.polygon,4326)) as area,
                                                         ST_Buffer_Meters(ST_Transform(model_draft.summary_de_hoes.polygon,4326), 75) as buffer_75
	                                            FROM model_draft.summary_de_hoes;

                 ALTER MATERIALIZED VIEW model_draft.buffer_75_hoes OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','buffer_75_hoes','ego_dp_substation_ehv.sql',' '); ''')


# create second view with same data to compare, grant (oeuser), scenario log
def create_view_buffer_75_a_hoes(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75_a_hoes CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.buffer_75_a_hoes 
                                               AS
	                                          SELECT osm_id, ST_Area(ST_Transform(model_draft.summary_de_hoes.polygon,4326)) as area_a,
                                                         ST_Buffer_Meters(ST_Transform(model_draft.summary_de_hoes.polygon,4326), 75) as buffer_75_a
	                                            FROM model_draft.summary_de_hoes;

                 ALTER MATERIALIZED VIEW model_draft.buffer_75_a_hoes OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','buffer_75_a_hoes','ego_dp_substation_ehv.sql',' '); ''')


# create view to eliminate smaller substations where buffers intersect, grant (oeuser), scenario log
def create_view_substations_to_drop_hoes(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.substations_to_drop_hoes CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.substations_to_drop_hoes 
                                               AS
	                                          SELECT DISTINCT
		                                         (CASE WHEN model_draft.buffer_75_hoes.area < model_draft.buffer_75_a_hoes.area_a 
                                                               THEN model_draft.buffer_75_hoes.osm_id 
                                                               ELSE model_draft.buffer_75_a_hoes.osm_id END) as osm_id,
		                                         (CASE WHEN model_draft.buffer_75_hoes.area < model_draft.buffer_75_a_hoes.area_a 
                                                               THEN model_draft.buffer_75_hoes.area 
                                                               ELSE model_draft.buffer_75_a_hoes.area_a END) as area,
		                                         (CASE WHEN model_draft.buffer_75_hoes.area < model_draft.buffer_75_a_hoes.area_a 
                                                               THEN model_draft.buffer_75_hoes.buffer_75 
                                                               ELSE model_draft.buffer_75_a_hoes.buffer_75_a END) as buffer
	                                            FROM model_draft.buffer_75_hoes, model_draft.buffer_75_a_hoes
	                                           WHERE ST_Intersects(model_draft.buffer_75_hoes.buffer_75, model_draft.buffer_75_a_hoes.buffer_75_a)
		                                         AND NOT model_draft.buffer_75_hoes.osm_id = model_draft.buffer_75_a_hoes.osm_id;
     
                 ALTER MATERIALIZED VIEW model_draft.substations_to_drop_hoes OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','substations_to_drop_hoes','ego_dp_substation_ehv.sql',' '); ''')


# filter those substations and create final_result_hoes, grant (oeuser), scenario log
def create_view_final_result_hoes(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP VIEW IF EXISTS model_draft.final_result_hoes CASCADE;
                         CREATE VIEW model_draft.final_result_hoes 
                                  AS
	                             SELECT * 
	                               FROM model_draft.summary_de_hoes
	                              WHERE model_draft.summary_de_hoes.osm_id 
                                            NOT IN 
                                            (SELECT model_draft.substations_to_drop_hoes.osm_id 
                                            FROM model_draft.substations_to_drop_hoes);

                 ALTER VIEW model_draft.final_result_hoes OWNER TO oeuser;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','temp','model_draft','final_result_hoes','ego_dp_substation_ehv.sql',' '); ''')


# insert results, scenario log
def insert_into_ego_grid_ehv_substation(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 INSERT INTO model_draft.ego_grid_ehv_substation (lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status)
	              SELECT lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status
	                FROM model_draft.final_result_hoes;

                 SELECT scenario_log('eGo_DP', 'v0.4.5','output','model_draft','ego_grid_ehv_substation','ego_dp_substation_ehv.sql',' '); ''')


# drop
def drop(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                              DROP VIEW IF EXISTS model_draft.final_result_hoes CASCADE;
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.substations_to_drop_hoes CASCADE;
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75_hoes CASCADE;
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75_a_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.summary_de_hoes CASCADE;
                 DROP MATERIALIZED VIEW IF EXISTS model_draft.summary_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.summary_total_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.substation_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.relation_substations_with_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.node_substations_with_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.way_substations_with_hoes CASCADE;
                              DROP VIEW IF EXISTS model_draft.way_substations CASCADE; ''')


dag_params = {
    'dag_id': 'ego_dp_substation_ehv',
    'start_date': datetime(2020, 7, 7),
    'schedule_interval': None
}


with DAG(**dag_params) as dag:

    create_table_ego_grid_ehv_substation = PythonOperator(
        task_id='create_table_ego_grid_ehv_substation',
        python_callable=create_table_ego_grid_ehv_substation
    )

    create_view_way_substations = PythonOperator(
        task_id='create_view_way_substations',
        python_callable=create_view_way_substations
    )

    create_view_way_substations_with_hoes = PythonOperator(
        task_id='create_view_way_substations_with_hoes',
        python_callable=create_view_way_substations_with_hoes
    )

    create_view_node_substations_with_hoes = PythonOperator(
        task_id='create_view_node_substations_with_hoes',
        python_callable=create_view_node_substations_with_hoes
    )

    create_view_relation_substations_with_hoes = PythonOperator(
        task_id='create_view_relation_substations_with_hoes',
        python_callable=create_view_relation_substations_with_hoes
    )

    create_view_substation_hoes = PythonOperator(
        task_id='create_view_substation_hoes',
        python_callable=create_view_substation_hoes
    )


    create_view_summary_total_hoes = PythonOperator(
        task_id='create_view_summary_total_hoes',
        python_callable=create_view_summary_total_hoes
    )


    create_view_summary_hoes = PythonOperator(
        task_id='create_view_summary_hoes',
        python_callable=create_view_summary_hoes
    )

    create_index_gist = PythonOperator(
        task_id='create_index_gist',
        python_callable=create_index_gist
    )


    create_view_summary_de_hoes = PythonOperator(
        task_id='create_view_summary_de_hoes',
        python_callable=create_view_summary_de_hoes
    )

    create_view_buffer_75_hoes = PythonOperator(
        task_id='create_view_buffer_75_hoes',
        python_callable=create_view_buffer_75_hoes
    )

    create_view_buffer_75_a_hoes = PythonOperator(
        task_id='create_view_buffer_75_a_hoes',
        python_callable=create_view_buffer_75_a_hoes
    )

    create_view_substations_to_drop_hoes = PythonOperator(
        task_id='create_view_substations_to_drop_hoes',
        python_callable=create_view_substations_to_drop_hoes
    )

    create_view_final_result_hoes = PythonOperator(
        task_id='create_view_final_result_hoes',
        python_callable=create_view_final_result_hoes
    )

    insert_into_ego_grid_ehv_substation = PythonOperator(
        task_id='insert_into_ego_grid_ehv_substation',
        python_callable=insert_into_ego_grid_ehv_substation
    )

    drop = PythonOperator(
        task_id='drop',
        python_callable=drop
    )

    trigger_next_dag =  TriggerDagRunOperator(
    trigger_dag_id='ego_dp_substation_otg', 
    task_id='update_ego_grid_hvmv_substation'
    )

    create_table_ego_grid_ehv_substation.set_downstream(create_view_way_substations)
    create_view_way_substations.set_downstream(create_view_way_substations_with_hoes)
    create_view_way_substations_with_hoes.set_downstream(create_view_node_substations_with_hoes)
    create_view_node_substations_with_hoes.set_downstream(create_view_relation_substations_with_hoes)
    create_view_relation_substations_with_hoes.set_downstream(create_view_substation_hoes)
    create_view_substation_hoes.set_downstream(create_view_summary_total_hoes)
    create_view_summary_total_hoes.set_downstream(create_view_summary_hoes)
    create_view_summary_hoes.set_downstream(create_index_gist)
    create_index_gist.set_downstream(create_view_summary_de_hoes)
    create_view_summary_de_hoes.set_downstream(create_view_buffer_75_hoes)
    create_view_buffer_75_hoes.set_downstream(create_view_buffer_75_a_hoes)
    create_view_buffer_75_a_hoes.set_downstream(create_view_substations_to_drop_hoes)
    create_view_substations_to_drop_hoes.set_downstream(create_view_final_result_hoes)
    create_view_final_result_hoes.set_downstream(insert_into_ego_grid_ehv_substation)
    insert_into_ego_grid_ehv_substation.set_downstream(drop)
    drop.set_downstream(trigger_next_dag)
