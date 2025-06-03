# # intersection_tables_manager
# import logging
# from sqlalchemy import text
# from sqlalchemy.exc import SQLAlchemyError

# from modules.infrastructure.other_ops.file_operations import read_yaml_file

# logger = logging.getLogger(__name__)

# class IntersectionTable:
#     """
#     Class to manage intersection tables in the database.

#     Attributes:
#         table_name (str): Name of the intersection table.
#         source_table (str): Name of the source table.
#         s_unique_id_col (str): Unique ID column in the source table.
#         s_geom_col_name (str): Geometry column name in the source table.
#         buffer_distance (float): Distance to buffer the geometry.
#         buf_quad_segs (int): Number of segments used to approximate a quarter circle.
#         hazards (list): List of Hazard objects.
#         db_engine (Engine): SQLAlchemy database engine.
#     """
#     def __init__(self, table_name, source_table, s_unique_id_col, s_geom_col_name, buffer_distance, buf_quad_segs, hazards, db_engine):
#         self.table_name = table_name
#         self.source_table = source_table
#         self.s_unique_id_col = s_unique_id_col
#         self.s_geom_col_name = s_geom_col_name
#         self.buffer_distance = buffer_distance
#         self.buf_geom_col_name = 'Geom_buff'
#         self.buf_quad_segs = buf_quad_segs
#         self.hazards = hazards
#         self.db_engine = db_engine

#     def update_source(self):
#         """
#         Updates the intersection table by dropping it if it exists and creating a new one.
#         The new table is populated from the source table with only the unique ID and buffered geometry columns.
#         """
#         logger.debug(f"Attempting to update source data for intersection table {self.table_name}.")
#         try:
#             with self.db_engine.connect() as connection:
#                 get_srid_sql = f"""
#                 SELECT ST_SRID({self.s_geom_col_name}) AS srid
#                 FROM {self.source_table}
#                 LIMIT 1;
#                 """
#                 result = connection.execute(text(get_srid_sql))
#                 srid = result.scalar()
#                 logger.debug(f"Retrieved SRID: {srid}")

#                 create_table_sql = f"""
#                 DROP TABLE IF EXISTS {self.table_name};
#                 CREATE TABLE {self.table_name} AS
#                 SELECT {self.s_unique_id_col}, 
#                        ST_Buffer({self.s_geom_col_name}, {self.buffer_distance}, 'quad_segs={self.buf_quad_segs}')::geometry(MULTIPOLYGON, {srid}) AS {self.buf_geom_col_name}
#                 FROM {self.source_table};

#                 CREATE INDEX {self.table_name}_geom_idx ON {self.table_name} USING GIST ({self.buf_geom_col_name});
#                 """
#                 logger.debug(f"Executing SQL: {create_table_sql}")
#                 connection.execute(text(create_table_sql))
#                 connection.commit()
#                 logger.debug(f"Source data for intersection table {self.table_name} has been successfully updated.")
#         except SQLAlchemyError as e:
#             logger.error(f"Error updating source data for table {self.table_name}: {e}")
#             raise

#     def run_intersection(self, hazard_name, join_table, j_geom_col_name, join_field_name, intersect_col_name):
#         """
#         Runs an intersection between the source and target tables and updates the target table with the results.

#         Args:
#             db_engine (Engine): SQLAlchemy engine connected to the database.
#             join_table (str): Name of the joining table.
#             j_geom_col_name (str): Name of the geometry column in the joining table.
#             target_table (str): Name of the target table.
#             t_geom_col_name (str): Name of the geometry column in the target table.
#             target_field (str): Name of the field in the joining table to be aggregated.
#             intersect_col_name (str): Name of the intersection column to be created or updated in the target table.

#         This function performs a spatial join between the joining and target tables using the ST_Intersects function.
#         The join is an inner join, meaning only rows with intersecting geometries will be considered.
#         The result in the intersection column will be an array of unique values from the target_field of the source table
#         for each row in the target table. If multiple intersecting features are found, the values in the target_field
#         of those intersecting features are put in the array. If there is a repeat of the same value, it will only appear
#         once in the array.
#         """
#         logger.debug(f"Attempting to run intersection for {self.table_name} with hazard {hazard_name}. This may take some time.")
#         try:
#             with self.db_engine.connect() as conn:
#                 # Intersection SQL query
#                 intersection_sql = text(f"""
#                     DO $$
#                     BEGIN
#                         -- Check if the intersection column exists and create it if it doesn't
#                         IF NOT EXISTS (
#                             SELECT 1 FROM information_schema.columns 
#                             WHERE table_name = '{self.table_name}' AND column_name = '{intersect_col_name}'
#                         ) THEN
#                             ALTER TABLE {self.table_name} ADD COLUMN {intersect_col_name} text[];
#                         END IF;

#                         -- Clear the existing column values
#                         UPDATE {self.table_name} SET {intersect_col_name} = NULL;

#                         -- Perform the intersection and update the target table
#                         UPDATE {self.table_name} AS t
#                         SET {intersect_col_name} = subquery.hazard_values
#                         FROM (
#                             SELECT t.{self.s_unique_id_col}, array_agg(DISTINCT j.{join_field_name}) AS hazard_values
#                             FROM {self.table_name} t
#                             JOIN {join_table} j
#                             ON ST_Intersects(t.{self.buf_geom_col_name}, j.{j_geom_col_name})
#                             GROUP BY t.{self.s_unique_id_col}
#                         ) AS subquery
#                         WHERE t.{self.s_unique_id_col} = subquery.{self.s_unique_id_col};
#                     END $$;
#                 """)
#                 conn.execute(intersection_sql)
#                 conn.commit()
#                 logger.debug(f"Updated column {intersect_col_name} in table {self.table_name} with intersection results")

#         except SQLAlchemyError as e:
#             logger.error(f"Failed to run intersection for {self.table_name}: {e}")
#             return False
#         return True

#     def filter_hazards(self, intersect_col_name, haz_vals_col_name, haz_val_class, haz_val_order, haz_threshold):
#         """
#         Filters out all results from the intersection that are not considered hazards.

#         Args:
#             intersect_col_name (str): The name of the column to read from.
#             haz_vals_col_name (str): The name of the column to write the filtered values to.
#             haz_val_class (str): The classification of the hazard values (ordinal, nominal, discrete, continuous).
#             haz_val_order (list or str): The order of the hazard values (list for ordinal/nominal, operator for discrete/continuous).
#             haz_threshold (list or str or int or float): The threshold value(s) for determining hazards.

#         Returns:
#             bool: True if successful, False otherwise.
#         """
#         logger.debug(f"Filtering hazards in table {self.table_name} for column {intersect_col_name}.")
#         try:
#             with self.db_engine.connect() as conn:
#                 # Check if the hazard values column exists, and create it if not
#                 check_column_sql = f"""
#                 DO $$
#                 BEGIN
#                     IF NOT EXISTS (
#                         SELECT 1 FROM information_schema.columns 
#                         WHERE table_name = '{self.table_name}' AND column_name = '{haz_vals_col_name}'
#                     ) THEN
#                         ALTER TABLE {self.table_name} ADD COLUMN {haz_vals_col_name} text[];
#                     ELSE
#                         UPDATE {self.table_name} SET {haz_vals_col_name} = NULL;
#                     END IF;
#                 END $$;
#                 """
#                 conn.execute(text(check_column_sql))

#                 # Filter based on hazard value classification
#                 if haz_val_class == 'ordinal':
#                     threshold_index = haz_val_order.index(haz_threshold)
#                     valid_values = haz_val_order[threshold_index:]
#                     filter_sql = f"""
#                     UPDATE {self.table_name}
#                     SET {haz_vals_col_name} = (
#                         SELECT array_agg(val)
#                         FROM unnest({intersect_col_name}) AS val
#                         WHERE val = ANY(:valid_values)
#                     );
#                     """
#                     conn.execute(text(filter_sql), {'valid_values': valid_values})

#                 elif haz_val_class == 'nominal':
#                     filter_sql = f"""
#                     UPDATE {self.table_name}
#                     SET {haz_vals_col_name} = (
#                         SELECT array_agg(val)
#                         FROM unnest({intersect_col_name}) AS val
#                         WHERE val = ANY(:haz_threshold)
#                     );
#                     """
#                     conn.execute(text(filter_sql), {'haz_threshold': haz_threshold})

#                 elif haz_val_class in ['discrete', 'continuous']:
#                     operator = haz_val_order
#                     if isinstance(haz_threshold, int):
#                         cast_type = 'int'
#                     else:
#                         cast_type = 'float'
#                     filter_sql = f"""
#                     UPDATE {self.table_name}
#                     SET {haz_vals_col_name} = (
#                         SELECT array_agg(val)
#                         FROM unnest({intersect_col_name}) AS val
#                         WHERE val::{cast_type} {operator} :haz_threshold
#                     );
#                     """
#                     conn.execute(text(filter_sql), {'haz_threshold': haz_threshold})

#                 conn.commit()
#                 logger.debug(f"Filtered hazards in table {self.table_name} for column {intersect_col_name}.")
#                 return True

#         except SQLAlchemyError as e:
#             logger.error(f"Error filtering hazards in table {self.table_name}: {e}")
#             return False

#     def determine_max_hazard_value(self, haz_val_col_name, max_col_name, haz_val_class, haz_val_order):
#         """
#         Determines the maximum hazard value based on the hazard value classification and updates the max_col_name.

#         Args:
#             haz_val_col_name (str): The name of the column containing hazard values.
#             max_col_name (str): The name of the column to write the maximum hazard value to.
#             haz_val_class (str): The classification of the hazard values (ordinal, nominal, discrete, continuous).
#             haz_val_order (list or str): The order of the hazard values (list for ordinal/nominal, operator for discrete/continuous).

#         Returns:
#             bool: True if successful, False otherwise.
#         """
#         logger.debug(f"Determining max hazard value in table {self.table_name} for column {haz_val_col_name}.")
#         try:
#             with self.db_engine.connect() as conn:
#                 # Determine the column type based on haz_val_class
#                 if haz_val_class in ['ordinal', 'nominal']:
#                     column_type = 'text'
#                 elif haz_val_class == 'discrete':
#                     column_type = 'int'
#                 elif haz_val_class == 'continuous':
#                     column_type = 'double precision'
#                 else:
#                     raise ValueError(f"Unknown haz_val_class: {haz_val_class}")

#                 # Update the column type
#                 alter_column_sql = f"""
#                 DO $$ 
#                 BEGIN
#                     IF EXISTS (
#                         SELECT 1 FROM information_schema.columns 
#                         WHERE table_name = '{self.table_name}' AND column_name = '{max_col_name}'
#                     ) THEN
#                         ALTER TABLE {self.table_name} ALTER COLUMN {max_col_name} TYPE {column_type} USING {max_col_name}::{column_type};
#                     ELSE
#                         ALTER TABLE {self.table_name} ADD COLUMN {max_col_name} {column_type};
#                     END IF;
#                 END $$;
#                 """
#                 conn.execute(text(alter_column_sql))

#                 if haz_val_class == 'ordinal':
#                     order_list = haz_val_order
#                     order_case = " ".join([f"WHEN '{val}' THEN {i}" for i, val in enumerate(order_list)])
#                     filter_sql = f"""
#                     UPDATE {self.table_name}
#                     SET {max_col_name} = (
#                         SELECT val
#                         FROM unnest({haz_val_col_name}) AS val
#                         ORDER BY CASE val {order_case} END DESC
#                         LIMIT 1
#                     )::text;
#                     """
#                     conn.execute(text(filter_sql))

#                 elif haz_val_class == 'nominal':
#                     filter_sql = f"""
#                     UPDATE {self.table_name}
#                     SET {max_col_name} = (
#                         SELECT string_agg(val, ',')
#                         FROM unnest({haz_val_col_name}) AS val
#                     )::text;
#                     """
#                     conn.execute(text(filter_sql))

#                 elif haz_val_class in ['discrete', 'continuous']:
#                     operator = haz_val_order
#                     if haz_val_class == 'discrete':
#                         cast_type = 'int'
#                     else:
#                         cast_type = 'double precision'
#                     sort_direction = 'DESC' if operator in ['>', '>='] else 'ASC'
#                     filter_sql = f"""
#                     UPDATE {self.table_name}
#                     SET {max_col_name} = (
#                         SELECT val::{cast_type}
#                         FROM unnest({haz_val_col_name}) AS val
#                         ORDER BY val::{cast_type} {sort_direction}
#                         LIMIT 1
#                     );
#                     """
#                     conn.execute(text(filter_sql))

#                 conn.commit()
#                 logger.debug(f"Determined max hazard value in table {self.table_name} for column {haz_val_col_name}.")
#                 return True

#         except (SQLAlchemyError, ValueError) as e:
#             logger.error(f"Error determining max hazard value in table {self.table_name}: {e}")
#             return False

#     def build_hazard_boolean_column(self, max_col_name, haz_bool_name):
#         """
#         Builds a boolean column based on the presence of non-null values in the max_col_name column.
#         Thus, a max_col_name column must be built before this function is called.

#         Args:
#             max_col_name (str): The name of the column containing maximum hazard values.
#             haz_bool_name (str): The name of the boolean column to be created.

#         Returns:
#             bool: True if successful, False otherwise.
#         """
#         logger.debug(f"Building hazard boolean column {haz_bool_name} in table {self.table_name}.")
#         try:
#             with self.db_engine.connect() as conn:
#                 # Check if the boolean column exists, and create it if not
#                 check_column_sql = f"""
#                 DO $$ 
#                 BEGIN
#                     IF NOT EXISTS (
#                         SELECT 1 FROM information_schema.columns 
#                         WHERE table_name = '{self.table_name}' AND column_name = '{haz_bool_name}'
#                     ) THEN
#                         ALTER TABLE {self.table_name} ADD COLUMN {haz_bool_name} boolean;
#                     ELSE
#                         UPDATE {self.table_name} SET {haz_bool_name} = NULL;
#                     END IF;
#                 END $$;
#                 """
#                 conn.execute(text(check_column_sql))

#                 # Update the boolean column based on the presence of non-null values in the max_col_name column
#                 update_sql = f"""
#                 UPDATE {self.table_name}
#                 SET {haz_bool_name} = CASE
#                     WHEN {max_col_name} IS NOT NULL THEN TRUE
#                     ELSE FALSE
#                 END;
#                 """
#                 conn.execute(text(update_sql))

#                 conn.commit()
#                 logger.debug(f"Built hazard boolean column {haz_bool_name} in table {self.table_name}.")
#                 return True

#         except SQLAlchemyError as e:
#             logger.error(f"Error building hazard boolean column {haz_bool_name} in table {self.table_name}: {e}")
#             return False

# class Hazard:
#     """
#     Class to hold information for the hazards that will be intersected.

#     Attributes:
#         source_table (str): Name of the source table.
#         s_geom_col_name (str): Geometry column name in the source table.
#         haz_field (str): Hazard field name.
#         haz_val_class (str): Hazard value classification.
#         haz_val_order (str): Hazard value order.
#         haz_threshold (str): Hazard value threshold.
#     """
#     def __init__(self, source_table, s_geom_col_name, haz_field, haz_val_class, haz_val_order, haz_threshold):
#         self.source_table = source_table
#         self.s_geom_col_name = s_geom_col_name
#         self.haz_field = haz_field
#         self.haz_val_class = haz_val_class
#         self.haz_val_order = haz_val_order
#         self.haz_threshold = haz_threshold

# class IntersectionTablesManager:
#     """
#     Class to manage the intersection tables configuration and database operations.

#     Attributes:
#         intersection_config (str): Path to the intersection configuration YAML file.
#         db_engine (Engine): SQLAlchemy database engine.
#         intersection_tables_config (dict): Dictionary containing intersection tables configuration.
#         hazards_config (dict): Dictionary containing hazards configuration.
#     """
#     def __init__(self, intersection_tables_config_path, intersection_col_names, db_engine):
#         self.intersection_config_path = intersection_tables_config_path
#         self.intersection_col_names = intersection_col_names
#         self.db_engine = db_engine
#         self.intersection_tables_config = None
#         self.hazards_config = None
#         self.intersection_tables = {}
#         self.hazards = {}
#         self._load_config()
#         self._initialize_intersection_tables()
#         self._initialize_hazards()

#     def _load_config(self):
#         """
#         Loads the intersection configuration from the YAML file.
#         """
#         try:
#             config = read_yaml_file(self.intersection_config_path)
#             self.intersection_tables_config = config.get('intersection_tables', {})
#             self.hazards_config = config.get('hazards', {})
#             logger.debug("Intersection configuration loaded successfully.")
#         except Exception as e:
#             logger.error(f"Error loading intersection configuration: {e}")
#             raise

#     def _initialize_intersection_tables(self):
#         """
#         Initializes IntersectionTable objects for every intersection table in the intersection_tables_config.
#         """
#         try:
#             for table_name, table_config in self.intersection_tables_config.items():
#                 intersection_table = IntersectionTable(
#                     table_name=table_name,
#                     source_table=table_config['source_table'],
#                     s_unique_id_col=table_config['source_unique_id_column'],
#                     s_geom_col_name=table_config['source_geometry_column'],
#                     buffer_distance=table_config['buffer_distance'],
#                     buf_quad_segs=table_config['buffer_quadrant_segments'],
#                     hazards=table_config['hazards'],
#                     db_engine=self.db_engine
#                 )
#                 self.intersection_tables[table_name] = intersection_table
#                 logger.debug(f"Intersection table {table_name} initialized successfully.")
#         except Exception as e:
#             logger.error(f"Error initializing intersection tables: {e}")
#             raise

#     def _initialize_hazards(self):
#         """
#         Initializes Hazard objects for every hazard in the hazards_config.
#         """
#         try:
#             for hazard_name, hazard_config in self.hazards_config.items():
#                 hazard = Hazard(
#                     source_table=hazard_config['source_table'],
#                     s_geom_col_name=hazard_config['source_geom_column'],
#                     haz_field=hazard_config['hazard_field'],
#                     haz_val_class=hazard_config['hazard_value_classification'],
#                     haz_val_order=hazard_config['hazard_values_order'],
#                     haz_threshold=hazard_config['hazard_value_threshold']
#                 )
#                 self.hazards[hazard_name] = hazard
#                 logger.debug(f"Hazard {hazard_name} initialized successfully.")
#         except Exception as e:
#             logger.error(f"Error initializing hazards: {e}")
#             raise

#     def update_sources(self, table_names=['update_all']):
#         """
#         Updates the sources for the specified intersection tables.

#         Args:
#             table_names (list): List of table names to update. Defaults to ['update_all'].
#         """
#         try:
#             if table_names == ['update_all']:
#                 table_names = list(self.intersection_tables.keys())
            
#             for table_name in table_names:
#                 if table_name in self.intersection_tables:
#                     self.intersection_tables[table_name].update_source()
#                 else:
#                     logger.warning(f"Intersection table {table_name} not found in configuration.")
#         except Exception as e:
#             logger.error(f"Error updating intersection table sources: {e}")
#             raise

#     def run_intersections(self, table_names=['intersect_all'], hazards=['all_hazards'], build_int_col=True, build_filter_col=True, build_max_col=True, build_max_all_col=True, build_bool_col=True):
#         """
#         Runs intersections for the specified intersection tables and hazards.
    
#         Args:
#             table_names (list): List of table names to run intersections for. Defaults to ['intersect_all'].
#             hazards (list): List of hazard names to run intersections for. Defaults to ['all_hazards'].
#         """
#         try:
#             if table_names == ['intersect_all']:
#                 table_names = list(self.intersection_tables.keys())
        
#             for table_name in table_names:
#                 if table_name in self.intersection_tables:
#                     intersection_table = self.intersection_tables[table_name]
#                     hazard_names = intersection_table.hazards if hazards == ['all_hazards'] else hazards
#                     for hazard_name in hazard_names:
#                         if hazard_name in self.hazards:
#                             hazard = self.hazards[hazard_name]
#                             intersect_col_name = hazard_name + self.intersection_col_names['intersect_col']
#                             haz_vals_col_name = hazard_name + self.intersection_col_names['haz_vals_col']
#                             max_col_name = hazard_name + self.intersection_col_names['max_col']
#                             max_all_col_name = hazard_name + self.intersection_col_names['max_all_col']
#                             bool_col_name = hazard_name + self.intersection_col_names['bool_col']
#                             if build_int_col:       
#                                 intersection_table.run_intersection(
#                                     hazard_name=hazard_name,
#                                     join_table=hazard.source_table,
#                                     j_geom_col_name=hazard.s_geom_col_name,
#                                     join_field_name=hazard.haz_field,
#                                     intersect_col_name=intersect_col_name
#                                 )
#                             if build_filter_col:                     
#                                 intersection_table.filter_hazards(
#                                     intersect_col_name=intersect_col_name, 
#                                     haz_vals_col_name=haz_vals_col_name, 
#                                     haz_val_class=hazard.haz_val_class,
#                                     haz_val_order=hazard.haz_val_order,
#                                     haz_threshold=hazard.haz_threshold
#                                 )
#                             if build_max_col:
#                                 intersection_table.determine_max_hazard_value(
#                                     haz_val_col_name=haz_vals_col_name,
#                                     max_col_name=max_col_name,
#                                     haz_val_class=hazard.haz_val_class,
#                                     haz_val_order=hazard.haz_val_order
#                                 )
#                             if build_max_all_col:
#                                 intersection_table.determine_max_hazard_value(
#                                     haz_val_col_name=intersect_col_name,
#                                     max_col_name=max_all_col_name,
#                                     haz_val_class=hazard.haz_val_class,
#                                     haz_val_order=hazard.haz_val_order
#                                 )
#                             if build_bool_col:
#                                 intersection_table.build_hazard_boolean_column(
#                                     max_col_name=max_col_name,
#                                     haz_bool_name=bool_col_name
#                                 )
#                         else:
#                             logger.warning(f"Hazard {hazard_name} not found in configuration.")
#                 else:
#                     logger.warning(f"Intersection table {table_name} not found in configuration.")
#         except Exception as e:
#             logger.error(f"Error running intersections: {e}")
#             raise

"""
intersection_tables_manager.py

Manages intersection tables and hazard configurations, and provides methods to update sources and run spatial intersections in the database.
"""

import logging
from typing import Dict, Any, List, Optional
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from modules.infrastructure.other_ops.file_operations import read_yaml_file

logger = logging.getLogger(__name__)

class IntersectionTable:
    """
    Manages an intersection table in the database.

    Attributes:
        table_name (str): Name of the intersection table.
        source_table (str): Name of the source table.
        s_unique_id_col (str): Unique ID column in the source table.
        s_geom_col_name (str): Geometry column name in the source table.
        buffer_distance (float): Distance to buffer the geometry.
        buf_quad_segs (int): Number of segments used to approximate a quarter circle.
        hazards (list): List of Hazard objects.
        db_engine (Engine): SQLAlchemy database engine.
    """
    def __init__(
        self,
        table_name: str,
        source_table: str,
        s_unique_id_col: str,
        s_geom_col_name: str,
        buffer_distance: float,
        buf_quad_segs: int,
        hazards: List[str],
        db_engine: Engine
    ) -> None:
        self.table_name = table_name
        self.source_table = source_table
        self.s_unique_id_col = s_unique_id_col
        self.s_geom_col_name = s_geom_col_name
        self.buffer_distance = buffer_distance
        self.buf_geom_col_name = 'Geom_buff'
        self.buf_quad_segs = buf_quad_segs
        self.hazards = hazards
        self.db_engine = db_engine

    def update_source(self) -> None:
        """
        Updates the intersection table by dropping it if it exists and creating a new one.
        The new table is populated from the source table with only the unique ID and buffered geometry columns.
        """
        logger.debug(f"Attempting to update source data for intersection table {self.table_name}.")
        try:
            with self.db_engine.connect() as connection:
                get_srid_sql = f"""
                SELECT ST_SRID({self.s_geom_col_name}) AS srid
                FROM {self.source_table}
                LIMIT 1;
                """
                result = connection.execute(text(get_srid_sql))
                srid = result.scalar()
                logger.debug(f"Retrieved SRID: {srid}")

                create_table_sql = f"""
                DROP TABLE IF EXISTS {self.table_name};
                CREATE TABLE {self.table_name} AS
                SELECT {self.s_unique_id_col}, 
                       ST_Buffer({self.s_geom_col_name}, {self.buffer_distance}, 'quad_segs={self.buf_quad_segs}')::geometry(MULTIPOLYGON, {srid}) AS {self.buf_geom_col_name}
                FROM {self.source_table};

                CREATE INDEX {self.table_name}_geom_idx ON {self.table_name} USING GIST ({self.buf_geom_col_name});
                """
                logger.debug(f"Executing SQL: {create_table_sql}")
                connection.execute(text(create_table_sql))
                connection.commit()
                logger.debug(f"Source data for intersection table {self.table_name} has been successfully updated.")
        except SQLAlchemyError as e:
            logger.error(f"Error updating source data for table {self.table_name}: {e}")
            raise

    def run_intersection(
        self,
        hazard_name: str,
        join_table: str,
        j_geom_col_name: str,
        join_field_name: str,
        intersect_col_name: str
    ) -> bool:
        """
        Runs an intersection between the source and target tables and updates the target table with the results.

        Returns:
            bool: True if successful, False otherwise.
        """
        logger.debug(f"Attempting to run intersection for {self.table_name} with hazard {hazard_name}. This may take some time.")
        try:
            with self.db_engine.connect() as conn:
                intersection_sql = text(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name = '{self.table_name}' AND column_name = '{intersect_col_name}'
                        ) THEN
                            ALTER TABLE {self.table_name} ADD COLUMN {intersect_col_name} text[];
                        END IF;

                        UPDATE {self.table_name} SET {intersect_col_name} = NULL;

                        UPDATE {self.table_name} AS t
                        SET {intersect_col_name} = subquery.hazard_values
                        FROM (
                            SELECT t.{self.s_unique_id_col}, array_agg(DISTINCT j.{join_field_name}) AS hazard_values
                            FROM {self.table_name} t
                            JOIN {join_table} j
                            ON ST_Intersects(t.{self.buf_geom_col_name}, j.{j_geom_col_name})
                            GROUP BY t.{self.s_unique_id_col}
                        ) AS subquery
                        WHERE t.{self.s_unique_id_col} = subquery.{self.s_unique_id_col};
                    END $$;
                """)
                conn.execute(intersection_sql)
                conn.commit()
                logger.debug(f"Updated column {intersect_col_name} in table {self.table_name} with intersection results")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to run intersection for {self.table_name}: {e}")
            return False

    def filter_hazards(
        self,
        intersect_col_name: str,
        haz_vals_col_name: str,
        haz_val_class: str,
        haz_val_order: Any,
        haz_threshold: Any
    ) -> bool:
        """
        Filters out all results from the intersection that are not considered hazards.

        Returns:
            bool: True if successful, False otherwise.
        """
        logger.debug(f"Filtering hazards in table {self.table_name} for column {intersect_col_name}.")
        try:
            with self.db_engine.connect() as conn:
                check_column_sql = f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = '{self.table_name}' AND column_name = '{haz_vals_col_name}'
                    ) THEN
                        ALTER TABLE {self.table_name} ADD COLUMN {haz_vals_col_name} text[];
                    ELSE
                        UPDATE {self.table_name} SET {haz_vals_col_name} = NULL;
                    END IF;
                END $$;
                """
                conn.execute(text(check_column_sql))

                if haz_val_class == 'ordinal':
                    threshold_index = haz_val_order.index(haz_threshold)
                    valid_values = haz_val_order[threshold_index:]
                    filter_sql = f"""
                    UPDATE {self.table_name}
                    SET {haz_vals_col_name} = (
                        SELECT array_agg(val)
                        FROM unnest({intersect_col_name}) AS val
                        WHERE val = ANY(:valid_values)
                    );
                    """
                    conn.execute(text(filter_sql), {'valid_values': valid_values})

                elif haz_val_class == 'nominal':
                    filter_sql = f"""
                    UPDATE {self.table_name}
                    SET {haz_vals_col_name} = (
                        SELECT array_agg(val)
                        FROM unnest({intersect_col_name}) AS val
                        WHERE val = ANY(:haz_threshold)
                    );
                    """
                    conn.execute(text(filter_sql), {'haz_threshold': haz_threshold})

                elif haz_val_class in ['discrete', 'continuous']:
                    operator = haz_val_order
                    cast_type = 'int' if isinstance(haz_threshold, int) else 'float'
                    filter_sql = f"""
                    UPDATE {self.table_name}
                    SET {haz_vals_col_name} = (
                        SELECT array_agg(val)
                        FROM unnest({intersect_col_name}) AS val
                        WHERE val::{cast_type} {operator} :haz_threshold
                    );
                    """
                    conn.execute(text(filter_sql), {'haz_threshold': haz_threshold})

                conn.commit()
                logger.debug(f"Filtered hazards in table {self.table_name} for column {intersect_col_name}.")
                return True

        except SQLAlchemyError as e:
            logger.error(f"Error filtering hazards in table {self.table_name}: {e}")
            return False

    def determine_max_hazard_value(
        self,
        haz_val_col_name: str,
        max_col_name: str,
        haz_val_class: str,
        haz_val_order: Any
    ) -> bool:
        """
        Determines the maximum hazard value based on the hazard value classification and updates the max_col_name.

        Returns:
            bool: True if successful, False otherwise.
        """
        logger.debug(f"Determining max hazard value in table {self.table_name} for column {haz_val_col_name}.")
        try:
            with self.db_engine.connect() as conn:
                if haz_val_class in ['ordinal', 'nominal']:
                    column_type = 'text'
                elif haz_val_class == 'discrete':
                    column_type = 'int'
                elif haz_val_class == 'continuous':
                    column_type = 'double precision'
                else:
                    raise ValueError(f"Unknown haz_val_class: {haz_val_class}")

                alter_column_sql = f"""
                DO $$ 
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = '{self.table_name}' AND column_name = '{max_col_name}'
                    ) THEN
                        ALTER TABLE {self.table_name} ALTER COLUMN {max_col_name} TYPE {column_type} USING {max_col_name}::{column_type};
                    ELSE
                        ALTER TABLE {self.table_name} ADD COLUMN {max_col_name} {column_type};
                    END IF;
                END $$;
                """
                conn.execute(text(alter_column_sql))

                if haz_val_class == 'ordinal':
                    order_list = haz_val_order
                    order_case = " ".join([f"WHEN '{val}' THEN {i}" for i, val in enumerate(order_list)])
                    filter_sql = f"""
                    UPDATE {self.table_name}
                    SET {max_col_name} = (
                        SELECT val
                        FROM unnest({haz_val_col_name}) AS val
                        ORDER BY CASE val {order_case} END DESC
                        LIMIT 1
                    )::text;
                    """
                    conn.execute(text(filter_sql))

                elif haz_val_class == 'nominal':
                    filter_sql = f"""
                    UPDATE {self.table_name}
                    SET {max_col_name} = (
                        SELECT string_agg(val, ',')
                        FROM unnest({haz_val_col_name}) AS val
                    )::text;
                    """
                    conn.execute(text(filter_sql))

                elif haz_val_class in ['discrete', 'continuous']:
                    operator = haz_val_order
                    cast_type = 'int' if haz_val_class == 'discrete' else 'double precision'
                    sort_direction = 'DESC' if operator in ['>', '>='] else 'ASC'
                    filter_sql = f"""
                    UPDATE {self.table_name}
                    SET {max_col_name} = (
                        SELECT val::{cast_type}
                        FROM unnest({haz_val_col_name}) AS val
                        ORDER BY val::{cast_type} {sort_direction}
                        LIMIT 1
                    );
                    """
                    conn.execute(text(filter_sql))

                conn.commit()
                logger.debug(f"Determined max hazard value in table {self.table_name} for column {haz_val_col_name}.")
                return True

        except (SQLAlchemyError, ValueError) as e:
            logger.error(f"Error determining max hazard value in table {self.table_name}: {e}")
            return False

    def build_hazard_boolean_column(
        self,
        max_col_name: str,
        haz_bool_name: str
    ) -> bool:
        """
        Builds a boolean column based on the presence of non-null values in the max_col_name column.

        Returns:
            bool: True if successful, False otherwise.
        """
        logger.debug(f"Building hazard boolean column {haz_bool_name} in table {self.table_name}.")
        try:
            with self.db_engine.connect() as conn:
                check_column_sql = f"""
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = '{self.table_name}' AND column_name = '{haz_bool_name}'
                    ) THEN
                        ALTER TABLE {self.table_name} ADD COLUMN {haz_bool_name} boolean;
                    ELSE
                        UPDATE {self.table_name} SET {haz_bool_name} = NULL;
                    END IF;
                END $$;
                """
                conn.execute(text(check_column_sql))

                update_sql = f"""
                UPDATE {self.table_name}
                SET {haz_bool_name} = CASE
                    WHEN {max_col_name} IS NOT NULL THEN TRUE
                    ELSE FALSE
                END;
                """
                conn.execute(text(update_sql))

                conn.commit()
                logger.debug(f"Built hazard boolean column {haz_bool_name} in table {self.table_name}.")
                return True

        except SQLAlchemyError as e:
            logger.error(f"Error building hazard boolean column {haz_bool_name} in table {self.table_name}: {e}")
            return False

class Hazard:
    """
    Holds information for the hazards that will be intersected.

    Attributes:
        source_table (str): Name of the source table.
        s_geom_col_name (str): Geometry column name in the source table.
        haz_field (str): Hazard field name.
        haz_val_class (str): Hazard value classification.
        haz_val_order (str): Hazard value order.
        haz_threshold (str): Hazard value threshold.
    """
    def __init__(
        self,
        source_table: str,
        s_geom_col_name: str,
        haz_field: str,
        haz_val_class: str,
        haz_val_order: Any,
        haz_threshold: Any
    ) -> None:
        self.source_table = source_table
        self.s_geom_col_name = s_geom_col_name
        self.haz_field = haz_field
        self.haz_val_class = haz_val_class
        self.haz_val_order = haz_val_order
        self.haz_threshold = haz_threshold

class IntersectionTablesManager:
    """
    Manages the intersection tables configuration and database operations.

    Attributes:
        intersection_config (str): Path to the intersection configuration YAML file.
        db_engine (Engine): SQLAlchemy database engine.
        intersection_tables_config (dict): Dictionary containing intersection tables configuration.
        hazards_config (dict): Dictionary containing hazards configuration.
    """
    def __init__(
        self,
        intersection_tables_config_path: str,
        intersection_col_names: Dict[str, str],
        db_engine: Engine
    ) -> None:
        self.intersection_config_path: str = intersection_tables_config_path
        self.intersection_col_names: Dict[str, str] = intersection_col_names
        self.db_engine: Engine = db_engine
        self.intersection_tables_config: Optional[Dict[str, Any]] = None
        self.hazards_config: Optional[Dict[str, Any]] = None
        self.intersection_tables: Dict[str, IntersectionTable] = {}
        self.hazards: Dict[str, Hazard] = {}
        self._load_config()
        self._initialize_intersection_tables()
        self._initialize_hazards()

    def _load_config(self) -> None:
        """
        Loads the intersection configuration from the YAML file.
        """
        try:
            config = read_yaml_file(self.intersection_config_path)
            self.intersection_tables_config = config.get('intersection_tables', {})
            self.hazards_config = config.get('hazards', {})
            logger.debug("Intersection configuration loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading intersection configuration: {e}")
            raise

    def _initialize_intersection_tables(self) -> None:
        """
        Initializes IntersectionTable objects for every intersection table in the intersection_tables_config.
        """
        try:
            for table_name, table_config in (self.intersection_tables_config or {}).items():
                intersection_table = IntersectionTable(
                    table_name=table_name,
                    source_table=table_config['source_table'],
                    s_unique_id_col=table_config['source_unique_id_column'],
                    s_geom_col_name=table_config['source_geometry_column'],
                    buffer_distance=table_config['buffer_distance'],
                    buf_quad_segs=table_config['buffer_quadrant_segments'],
                    hazards=table_config['hazards'],
                    db_engine=self.db_engine
                )
                self.intersection_tables[table_name] = intersection_table
                logger.debug(f"Intersection table {table_name} initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing intersection tables: {e}")
            raise

    def _initialize_hazards(self) -> None:
        """
        Initializes Hazard objects for every hazard in the hazards_config.
        """
        try:
            for hazard_name, hazard_config in (self.hazards_config or {}).items():
                hazard = Hazard(
                    source_table=hazard_config['source_table'],
                    s_geom_col_name=hazard_config['source_geom_column'],
                    haz_field=hazard_config['hazard_field'],
                    haz_val_class=hazard_config['hazard_value_classification'],
                    haz_val_order=hazard_config['hazard_values_order'],
                    haz_threshold=hazard_config['hazard_value_threshold']
                )
                self.hazards[hazard_name] = hazard
                logger.debug(f"Hazard {hazard_name} initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing hazards: {e}")
            raise

    def update_sources(self, table_names: Optional[List[str]] = None) -> None:
        """
        Updates the sources for the specified intersection tables.

        Args:
            table_names (Optional[List[str]]): List of table names to update. If None or ['update_all'], all tables are updated.
        """
        try:
            if table_names is None or table_names == ['update_all']:
                table_names = list(self.intersection_tables.keys())

            for table_name in table_names:
                if table_name in self.intersection_tables:
                    self.intersection_tables[table_name].update_source()
                else:
                    logger.warning(f"Intersection table {table_name} not found in configuration.")
        except Exception as e:
            logger.error(f"Error updating intersection table sources: {e}")
            raise

    def run_intersections(
        self,
        table_names: Optional[List[str]] = None,
        hazards: Optional[List[str]] = None,
        build_int_col: bool = True,
        build_filter_col: bool = True,
        build_max_col: bool = True,
        build_max_all_col: bool = True,
        build_bool_col: bool = True
    ) -> None:
        """
        Runs intersections for the specified intersection tables and hazards.

        Args:
            table_names (Optional[List[str]]): List of table names to run intersections for. If ['intersect_all'], all tables are used. If None, none are run.
            hazards (Optional[List[str]]): List of hazard names to run intersections for. If ['all_hazards'], all hazards for the table are used. If None, none are run.
        """
        try:
            if table_names is None:
                logger.info("No intersection tables specified. No intersections will be run.")
                return
            if table_names == ['intersect_all']:
                table_names = list(self.intersection_tables.keys())

            for table_name in table_names:
                if table_name in self.intersection_tables:
                    intersection_table = self.intersection_tables[table_name]
                    if hazards is None:
                        logger.info(f"No hazards specified for table {table_name}. No intersections will be run for this table.")
                        continue
                    hazard_names = intersection_table.hazards if 'all_hazards' in hazards else hazards
                    for hazard_name in hazard_names:
                        if hazard_name in self.hazards:
                            hazard = self.hazards[hazard_name]
                            intersect_col_name = hazard_name + self.intersection_col_names['intersect_col']
                            haz_vals_col_name = hazard_name + self.intersection_col_names['haz_vals_col']
                            max_col_name = hazard_name + self.intersection_col_names['max_col']
                            max_all_col_name = hazard_name + self.intersection_col_names['max_all_col']
                            bool_col_name = hazard_name + self.intersection_col_names['bool_col']
                            if build_int_col:       
                                intersection_table.run_intersection(
                                    hazard_name=hazard_name,
                                    join_table=hazard.source_table,
                                    j_geom_col_name=hazard.s_geom_col_name,
                                    join_field_name=hazard.haz_field,
                                    intersect_col_name=intersect_col_name
                                )
                            if build_filter_col:                     
                                intersection_table.filter_hazards(
                                    intersect_col_name=intersect_col_name, 
                                    haz_vals_col_name=haz_vals_col_name, 
                                    haz_val_class=hazard.haz_val_class,
                                    haz_val_order=hazard.haz_val_order,
                                    haz_threshold=hazard.haz_threshold
                                )
                            if build_max_col:
                                intersection_table.determine_max_hazard_value(
                                    haz_val_col_name=haz_vals_col_name,
                                    max_col_name=max_col_name,
                                    haz_val_class=hazard.haz_val_class,
                                    haz_val_order=hazard.haz_val_order
                                )
                            if build_max_all_col:
                                intersection_table.determine_max_hazard_value(
                                    haz_val_col_name=intersect_col_name,
                                    max_col_name=max_all_col_name,
                                    haz_val_class=hazard.haz_val_class,
                                    haz_val_order=hazard.haz_val_order
                                )
                            if build_bool_col:
                                intersection_table.build_hazard_boolean_column(
                                    max_col_name=max_col_name,
                                    haz_bool_name=bool_col_name
                                )
                        else:
                            logger.warning(f"Hazard {hazard_name} not found in configuration.")
                else:
                    logger.warning(f"Intersection table {table_name} not found in configuration.")
        except Exception as e:
            logger.error(f"Error running intersections: {e}")
            raise