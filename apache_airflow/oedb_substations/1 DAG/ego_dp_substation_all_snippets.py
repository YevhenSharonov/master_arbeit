from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# HVMV Substation
# Abstract HVMV Substations of the high voltage level from OSM.
# This script abstracts substations of the high voltage level from openstreetmap data.
# All substations that are relevant transition points between the transmission and distribution grid are identified, irrelevant ones are disregarded.
#
# __copyright__   = "DLR Institute for Networked Energy Systems"
# __license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
# __url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
# __author__      = "lukasol, C. Matke, Ludee"


def ego_dp_substation_hvmv(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''
                 DROP TABLE IF EXISTS model_draft.ego_grid_hvmv_substation CASCADE;
                         CREATE TABLE model_draft.ego_grid_hvmv_substation (
	                     subst_id   serial NOT NULL,
	                     lon        float NOT NULL,
	                     lat        float NOT NULL,
	                     point      geometry(Point,4326) NOT NULL,
	                     polygon    geometry NOT NULL,	
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

                 DROP VIEW IF EXISTS model_draft.way_substations_with_110kV CASCADE;
                         CREATE VIEW model_draft.way_substations_with_110kV 
                                  AS
	                             SELECT * 
	                               FROM model_draft.way_substations
	                              WHERE '110000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';')) 
		                            OR '60000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';'));

                 ALTER VIEW model_draft.way_substations_with_110kV OWNER TO oeuser;

                 DROP VIEW IF EXISTS model_draft.way_substations_without_110kV CASCADE;
                         CREATE VIEW model_draft.way_substations_without_110kV 
                                  AS
                                     SELECT * 
                                       FROM model_draft.way_substations
	                              WHERE not '110000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';')) 
		                            AND not '60000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';')) 
		                            OR not exist(hstore(model_draft.way_substations.tags),'voltage');

                 ALTER VIEW model_draft.way_substations_without_110kV OWNER TO oeuser;

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

                 DROP VIEW IF EXISTS model_draft.way_substations_without_110kV_intersected_by_110kV_line CASCADE;
                         CREATE VIEW model_draft.way_substations_without_110kV_intersected_by_110kV_line 
                                  AS
	                             SELECT DISTINCT model_draft.way_substations_without_110kV.* 
	                               FROM model_draft.way_substations_without_110kV, model_draft.way_lines_110kV
	                              WHERE ST_Contains(model_draft.way_substations_without_110kV.geom,ST_StartPoint(model_draft.way_lines_110kV.geom)) 
		                            OR ST_Contains(model_draft.way_substations_without_110kV.geom,ST_EndPoint(model_draft.way_lines_110kV.geom));

                 ALTER VIEW model_draft.way_substations_without_110kV_intersected_by_110kV_line OWNER TO oeuser;

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

                 ALTER VIEW model_draft.summary_total OWNER TO oeuser;

                 DROP MATERIALIZED VIEW IF EXISTS model_draft.summary CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.summary 
                                               AS
	                                          SELECT *
	                                            FROM model_draft.summary_total
	                                           WHERE dbahn = 'no' AND substation NOT IN ('traction','transition');

                 CREATE INDEX summary_gix ON model_draft.summary USING GIST (polygon);

                 ALTER MATERIALIZED VIEW model_draft.summary OWNER TO oeuser;

                 DROP VIEW IF EXISTS model_draft.summary_de CASCADE;
                         CREATE VIEW model_draft.summary_de 
                                  AS
	                             SELECT *
	                               FROM model_draft.summary, boundaries.bkg_vg250_1_sta_union_mview as vg
	                              WHERE ST_Transform(vg.geom,4326) && model_draft.summary.polygon 
	                                    AND ST_CONTAINS(ST_Transform(vg.geom,4326),model_draft.summary.polygon);

                 ALTER VIEW model_draft.summary_de OWNER TO oeuser;

                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75 CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.buffer_75 
                                               AS
	                                          SELECT osm_id, ST_Area(ST_Transform(model_draft.summary_de.polygon,4326)) as area, 
                                                         ST_Buffer_Meters(ST_Transform(model_draft.summary_de.polygon,4326), 75) as buffer_75
	                                            FROM model_draft.summary_de;

                 ALTER MATERIALIZED VIEW model_draft.buffer_75 OWNER TO oeuser;

                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75_a CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.buffer_75_a 
                                               AS
	                                          SELECT osm_id, ST_Area(ST_Transform(model_draft.summary_de.polygon,4326)) as area_a, 
                                                         ST_Buffer_Meters(ST_Transform(model_draft.summary_de.polygon,4326), 75) as buffer_75_a
	                                            FROM model_draft.summary_de;

                 ALTER MATERIALIZED VIEW model_draft.buffer_75_a OWNER TO oeuser;

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

                 DROP VIEW IF EXISTS model_draft.final_result CASCADE;
                         CREATE VIEW model_draft.final_result 
                                  AS
	                             SELECT * 
	                             FROM model_draft.summary_de
	                             WHERE model_draft.summary_de.osm_id 
                                           NOT IN ( SELECT model_draft.substations_to_drop.osm_id FROM model_draft.substations_to_drop);

                 ALTER VIEW model_draft.final_result OWNER TO oeuser;

                 INSERT INTO model_draft.ego_grid_hvmv_substation (lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status)
	              SELECT lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status
	                FROM model_draft.final_result;

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


def ego_dp_substation_ehv(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
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
	             "version": "v0.4.5" }' ;

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

                 DROP VIEW IF EXISTS model_draft.way_substations_with_hoes CASCADE;
                         CREATE VIEW model_draft.way_substations_with_hoes 
                                  AS
	                             SELECT * 
	                               FROM model_draft.way_substations
	                              WHERE '220000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';')) 
		                            OR '380000' = ANY( string_to_array(hstore(model_draft.way_substations.tags)->'voltage',';'));

                 ALTER VIEW model_draft.way_substations_with_hoes OWNER TO oeuser;

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

                 DROP MATERIALIZED VIEW IF EXISTS model_draft.summary_hoes CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.summary_hoes 
                                               AS
	                                          SELECT *
	                                            FROM model_draft.summary_total_hoes
	                                           WHERE dbahn = 'no' AND substation NOT IN ('traction','transition');

                 ALTER MATERIALIZED VIEW model_draft.summary_hoes OWNER TO oeuser;

                 CREATE INDEX summary_hoes_gix ON model_draft.summary_hoes USING GIST (polygon);

                 DROP VIEW IF EXISTS model_draft.summary_de_hoes CASCADE;
                         CREATE VIEW model_draft.summary_de_hoes 
                                  AS
	                             SELECT *
	                               FROM model_draft.summary_hoes, boundaries.bkg_vg250_1_sta_union_mview as vg
	                              WHERE ST_Transform(vg.geom,4326) && model_draft.summary_hoes.polygon 
	                                    AND ST_CONTAINS(ST_Transform(vg.geom,4326),model_draft.summary_hoes.polygon);

                 ALTER VIEW model_draft.summary_de_hoes OWNER TO oeuser;

                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75_hoes CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.buffer_75_hoes 
                                               AS
	                                          SELECT osm_id, ST_Area(ST_Transform(model_draft.summary_de_hoes.polygon,4326)) as area,
                                                         ST_Buffer_Meters(ST_Transform(model_draft.summary_de_hoes.polygon,4326), 75) as buffer_75
	                                            FROM model_draft.summary_de_hoes;

                 ALTER MATERIALIZED VIEW model_draft.buffer_75_hoes OWNER TO oeuser;

                 DROP MATERIALIZED VIEW IF EXISTS model_draft.buffer_75_a_hoes CASCADE;
                         CREATE MATERIALIZED VIEW model_draft.buffer_75_a_hoes 
                                               AS
	                                          SELECT osm_id, ST_Area(ST_Transform(model_draft.summary_de_hoes.polygon,4326)) as area_a,
                                                         ST_Buffer_Meters(ST_Transform(model_draft.summary_de_hoes.polygon,4326), 75) as buffer_75_a
	                                            FROM model_draft.summary_de_hoes;

                 ALTER MATERIALIZED VIEW model_draft.buffer_75_a_hoes OWNER TO oeuser;

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

                 INSERT INTO model_draft.ego_grid_ehv_substation (lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status)
	              SELECT lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status
	                FROM model_draft.final_result_hoes;

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


def ego_dp_substation_otg(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''

                 ALTER TABLE model_draft.ego_grid_hvmv_substation 
	                 ADD COLUMN otg_id bigint;

                 UPDATE model_draft.ego_grid_hvmv_substation
	            SET otg_id = grid.otg_ehvhv_bus_data.bus_i
	           FROM grid.otg_ehvhv_bus_data
	          WHERE grid.otg_ehvhv_bus_data.base_kv <= 110 AND 
                        (SELECT TRIM(leading 'n' 
                        FROM TRIM(leading 'w' 
                        FROM model_draft.ego_grid_hvmv_substation.osm_id))::BIGINT)=grid.otg_ehvhv_bus_data.osm_substation_id;

                 DELETE FROM model_draft.ego_grid_hvmv_substation WHERE otg_id IS NULL;

                 ALTER TABLE model_draft.ego_grid_ehv_substation
	                 ADD COLUMN otg_id bigint;

                 UPDATE model_draft.ego_grid_ehv_substation
	            SET otg_id = grid.otg_ehvhv_bus_data.bus_i
	           FROM grid.otg_ehvhv_bus_data
	          WHERE grid.otg_ehvhv_bus_data.base_kv > 110 AND
                        (SELECT TRIM(leading 'n' 
                        FROM TRIM(leading 'w' FROM TRIM(leading 'r' 
                        FROM model_draft.ego_grid_ehv_substation.osm_id)))::BIGINT)=grid.otg_ehvhv_bus_data.osm_substation_id;

                 DELETE FROM model_draft.ego_grid_ehv_substation WHERE otg_id IS NULL; ''')


def ego_dp_substation_hvmv_voronoi(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''

                           ALTER TABLE model_draft.ego_grid_hvmv_substation
	         DROP COLUMN IF EXISTS ags_0 CASCADE,
	                    ADD COLUMN ags_0 text,
	         DROP COLUMN IF EXISTS geom CASCADE,
	                    ADD COLUMN geom geometry(Point,3035);

                 UPDATE model_draft.ego_grid_hvmv_substation t1
     	            SET geom = ST_TRANSFORM(t1.point,3035)
	           FROM model_draft.ego_grid_hvmv_substation t2
	          WHERE t1.subst_id = t2.subst_id;

                 CREATE INDEX ego_grid_hvmv_substation_geom_idx
	                 ON model_draft.ego_grid_hvmv_substation USING GIST (geom);

                 UPDATE model_draft.ego_grid_hvmv_substation AS t1
	            SET ags_0 = t2.ags_0
	           FROM (SELECT	sub.subst_id AS subst_id, vg.ags_0 AS ags_0
		        FROM model_draft.ego_grid_hvmv_substation AS sub, model_draft.ego_boundaries_bkg_vg250_6_gem_clean AS vg
		        WHERE vg.geom && sub.geom AND ST_CONTAINS(vg.geom,sub.geom)
		        ) AS t2
	          WHERE t1.subst_id = t2.subst_id;

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

                 SELECT obj_description('model_draft.ego_grid_hvmv_substation_dummy' ::regclass) ::json;

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


def ego_dp_substation_ehv_voronoi(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_oedb')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        result = connection.execute('''

                 INSERT INTO model_draft.ego_grid_ehv_substation (subst_name, point, subst_id, otg_id, lon, lat, polygon, osm_id, osm_www, status)
                      SELECT 'DUMMY', ST_TRANSFORM(geom,4326), subst_id, subst_id, ST_X (ST_Transform (geom, 4326)), ST_Y (ST_Transform (geom, 4326)), 
                             'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', 'dummy'||row_number() OVER(), 'dummy', 0
                        FROM model_draft.ego_grid_hvmv_substation_dummy;

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
	                          "version": "v0.4.2" }' ; ''')







dag_params = {
    'dag_id': 'ego_dp_substation_all_snippets',
    'start_date': datetime(2020, 7, 7),
    'schedule_interval': None
}


with DAG(**dag_params) as dag:

    ego_dp_substation_hvmv = PythonOperator(
        task_id='ego_dp_substation_hvmv',
        python_callable=ego_dp_substation_hvmv
    )

    ego_dp_substation_ehv = PythonOperator(
        task_id='ego_dp_substation_ehv',
        python_callable=ego_dp_substation_ehv
    )

    ego_dp_substation_otg = PythonOperator(
        task_id='ego_dp_substation_otg',
        python_callable=ego_dp_substation_otg
    )

    ego_dp_substation_hvmv_voronoi = PythonOperator(
        task_id='ego_dp_substation_hvmv_voronoi',
        python_callable=ego_dp_substation_hvmv_voronoi
    )

    ego_dp_substation_ehv_voronoi = PythonOperator(
        task_id='ego_dp_substation_ehv_voronoi',
        python_callable=ego_dp_substation_ehv_voronoi
    )

    
    ego_dp_substation_hvmv >> ego_dp_substation_ehv >> ego_dp_substation_otg >> ego_dp_substation_hvmv_voronoi >> ego_dp_substation_ehv_voronoi

