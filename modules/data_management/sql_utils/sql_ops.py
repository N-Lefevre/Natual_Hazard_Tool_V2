import logging
import json
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from random import getrandbits
import os
import subprocess
from urllib.parse import quote
import shutil
from osgeo import gdal

logger = logging.getLogger(__name__)

def execute_sql_operations(db_engine, operations):
    """
    Executes a list of SQL operations in order.

    Args:
        db_engine (Engine): SQLAlchemy database engine.
        operations (list): List of SQL operations to execute.

    Returns:
        bool: True if all operations are executed successfully, False otherwise.
    """
    try:
        for operation in operations:
            for func_name, params in operation.items():
                func = globals().get(func_name)
                if func is None:
                    logger.error(f"Function {func_name} not found in sql_ops")
                    return False
                logger.debug(f"Executing {func_name}")
                func(db_engine, **params)
        return True
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemyError during execution: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during execution: {e}")
        return False

# def create_engine_with_extensions(db_url):
#     """
#     Creates a SQLAlchemy engine for the given database URL and loads the appropriate extensions.

#     Args:
#         db_url (str): The database connection URL.

#     Returns:
#         Engine: SQLAlchemy engine connected to the database.
#     """
#     try:
#         engine = create_engine(db_url)
#         logger.debug("Engine created")
#     except Exception as e:
#         logger.error(f"Failed to create engine: {e}")
#         raise

#     with engine.connect() as conn:
#         result = conn.execute(text("SELECT PostGIS_Version();"))
#         logger.debug(f"PostGIS version: {result.fetchone()}")

#     return engine

def create_engine_with_extensions(db_url: str) -> Engine:
    """
    Creates a SQLAlchemy engine for the given database URL and loads the appropriate extensions.

    Args:
        db_url (str): The database connection URL.

    Returns:
        Engine: SQLAlchemy engine connected to the database.

    Raises:
        Exception: If the engine cannot be created or the connection fails.
    """
    try:
        engine = create_engine(db_url)
        logger.debug("Engine created")
    except Exception as e:
        logger.error(f"Failed to create engine: {e}")
        raise

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT PostGIS_Version();"))
            logger.debug(f"PostGIS version: {result.fetchone()}")
    except Exception as e:
        logger.error(f"Failed to connect or execute PostGIS version check: {e}")
        raise

    return engine

def add_column(db_engine, table_name, column_name, column_type):
    """
    Adds a column to the specified table.

    Args:
        db_engine (Engine): SQLAlchemy engine connected to the database.
        table_name (str): Name of the table to add the column to.
        column_name (str): Name of the column to add.
        column_type (str): Data type of the column to add.
    """
    try:
        with db_engine.connect() as conn:
            add_column_sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
            conn.execute(text(add_column_sql))
            conn.commit()
            logger.debug(f"Added column {column_name} to table {table_name}")
    except SQLAlchemyError as e:
        logger.error(f"Failed to add column {column_name} to table {table_name}: {e}")
        return False
    return True

def remove_columns(db_engine, table_name, remove_columns, remove_columns_trails=None):
    """
    Removes specified columns from the table. Optionally removes columns ending with specified suffixes.

    Args:
        db_engine (Engine): SQLAlchemy engine connected to the database.
        table_name (str): Name of the table to remove columns from.
        remove_columns (list): List of column names to remove.
        remove_trails (list, optional): List of suffixes to remove columns ending with these strings. Defaults to None.

    Returns:
        bool: True if columns are removed successfully, False otherwise.
    """
    try:
        with db_engine.connect() as conn:
            # Remove specified columns
            for column_name in remove_columns:
                remove_column_sql = f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name}"
                conn.execute(text(remove_column_sql))
                logger.debug(f"Removed column {column_name} from table {table_name}")

            # Optionally remove columns ending with specified suffixes
            if remove_columns_trails:
                for suffix in remove_columns_trails:
                    remove_trail_sql = f"""
                    DO $$
                    DECLARE
                        r RECORD;
                    BEGIN
                        FOR r IN (SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name LIKE '%{suffix}') LOOP
                            EXECUTE 'ALTER TABLE {table_name} DROP COLUMN ' || r.column_name;
                        END LOOP;
                    END $$;
                    """
                    conn.execute(text(remove_trail_sql))
                    logger.debug(f"Removed columns ending with {suffix} from table {table_name}")

            conn.commit()
            return True

    except SQLAlchemyError as e:
        logger.error(f"Failed to remove columns from table {table_name}: {e}")
        return False

def clear_column(db_engine, table_name, column_name):
    """
    Clears all values in the specified column of the table.

    Args:
        db_engine (Engine): SQLAlchemy engine connected to the database.
        table_name (str): Name of the table.
        column_name (str): Name of the column to clear.
    """
    try:
        with db_engine.connect() as conn:
            clear_column_sql = text(f"UPDATE {table_name} SET {column_name} = NULL")
            conn.execute(clear_column_sql)
            conn.commit()
            logger.debug(f"Cleared column {column_name} in table {table_name}")
    except SQLAlchemyError as e:
        logger.error(f"Failed to clear column {column_name} in table {table_name}: {e}")
        return False
    return True

def update_primary_key(db_engine, table_name, primary_key):
    """
    Updates the primary key of the specified table.

    Args:
        db_engine (Engine): SQLAlchemy engine connected to the database.
        table_name (str): Name of the table to update the primary key.
        primary_key (str): Name of the primary key column.
    """
    try:
        with db_engine.connect() as conn:
            # Drop existing primary key constraint
            drop_pk_sql = f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {table_name}_pkey"
            conn.execute(text(drop_pk_sql))
            
            # Add new primary key constraint
            add_pk_sql = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key})"
            conn.execute(text(add_pk_sql))
            conn.commit()
            logger.debug(f"Updated primary key to {primary_key} for table {table_name}")
    except SQLAlchemyError as e:
        logger.error(f"Failed to update primary key for table {table_name}: {e}")
        return False
    return True

def drop_and_rebuild_table(db_engine, table_name, table_columns={}):
    """
    Drops and rebuilds the table with the specified columns and primary key.

    Args:
        db_engine (Engine): SQLAlchemy engine connected to the database.
        table_name (str): Name of the table to drop and rebuild.
        table_columns (dict): Dictionary of column names and their data types.
        primary_key (str): Name of the primary key column.

    Returns:
        bool: True if the table is dropped and rebuilt successfully, False otherwise.
    """
    try:
        with db_engine.connect() as conn:
            # Check if the table exists
            table_exists_query = text(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = :table_name
                )
                """
            )
            table_exists = conn.execute(table_exists_query, {"table_name": table_name}).scalar()
            
            if table_exists:
                # Drop the table
                drop_table_sql = f"DROP TABLE {table_name}"
                conn.execute(text(drop_table_sql))
                conn.commit()
                logger.debug(f"Table {table_name} dropped successfully")
            
            create_table_sql = f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY);"
            conn.execute(text(create_table_sql))
            conn.commit()
            logger.debug(f"Table {table_name} created successfully")
            
            # Add columns
            for column_name, column_type in table_columns.items():
                if not add_column(db_engine, table_name, column_name, column_type):
                    return False
            
            # Update primary key
            #if not update_primary_key(db_engine, table_name, primary_key):
                #return False

        return True
    except Exception as e:
        logger.error(f"Failed to drop and rebuild table {table_name}: {e}")
        return False

def clear_table(db_engine, table_name):
    """
    Clears the table in the database.

    Args:
        db_engine (Engine): SQLAlchemy engine connected to the database.
        table_name (str): Name of the table to clear.

    Returns:
        bool: True if the table is cleared successfully, False otherwise.
    """
    try:
        with db_engine.connect() as conn:
            clear_table_sql = f"TRUNCATE TABLE {table_name}"
            conn.execute(text(clear_table_sql))
            conn.commit()
            logger.debug(f"Cleared table {table_name}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"Failed to clear table {table_name}: {e}")
        return False

def geojson_to_postgis(db_engine, table_name, columns, features):
    """Insert GeoJSON features into a specified PostGIS table dynamically."""
    try:
        with db_engine.connect() as conn:
            for feature in features:
                geojson_geom = json.dumps(feature["geometry"])  # Convert geometry to JSON string
                props = feature["properties"]

                # Modify placeholders: Apply ST_GeomFromGeoJSON only to the geometry column
                placeholders = [f"ST_SetSRID(ST_GeomFromGeoJSON(:{col}), 4326)" if col == "geometry" else f":{col}" for col in columns]
                #placeholders = [f"ST_SetSRID(ST_GeomFromGeoJSON(:{col}), 3857)" if col == "geometry" else f":{col}" for col in columns]
            
                sql = text(f"""
                    INSERT INTO {table_name} ({', '.join(columns)}) 
                    VALUES ({', '.join(placeholders)})
                """)

                # Create dictionary of parameters, ensuring missing values are handled
                params = {col: (geojson_geom if col == "geometry" else props.get(col)) for col in columns}
            
                conn.execute(sql, params)

            conn.commit()
        logger.debug(f"Inserted {len(features)} features into table {table_name}")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Failed to insert features into table {table_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during insertion: {e}")
        return False

def run_intersection(db_engine, join_table, j_geom_col_name, target_table, t_geom_col_name, target_field, intersect_col_name):
    """
    Runs an intersection between the source and target tables and updates the target table with the results.

    Args:
        db_engine (Engine): SQLAlchemy engine connected to the database.
        join_table (str): Name of the joining table.
        j_geom_col_name (str): Name of the geometry column in the joining table.
        target_table (str): Name of the target table.
        t_geom_col_name (str): Name of the geometry column in the target table.
        target_field (str): Name of the field in the joining table to be aggregated.
        intersect_col_name (str): Name of the intersection column to be created or updated in the target table.

    This function performs a spatial join between the joining and target tables using the ST_Intersects function.
    The join is an inner join, meaning only rows with intersecting geometries will be considered.
    The result in the intersection column will be an array of unique values from the target_field of the source table
    for each row in the target table. If multiple intersecting features are found, the values in the target_field
    of those intersecting features are put in the array. If there is a repeat of the same value, it will only appear
    once in the array.
    """
    try:
        with db_engine.connect() as conn:
            # Intersection SQL query
            intersection_sql = text(f"""
                DO $$
                BEGIN
                    -- Check if the intersection column exists and create it if it doesn't
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = '{target_table}' AND column_name = '{intersect_col_name}'
                    ) THEN
                        ALTER TABLE {target_table} ADD COLUMN {intersect_col_name} text[];
                    END IF;

                    -- Clear the existing column values
                    UPDATE {target_table} SET {intersect_col_name} = NULL;

                    -- Perform the intersection and update the target table
                    UPDATE {target_table} AS t
                    SET {intersect_col_name} = subquery.hazard_values
                    FROM (
                        SELECT t.id, array_agg(DISTINCT s.{target_field}) AS hazard_values
                        FROM {target_table} t
                        JOIN {join_table} j
                        ON ST_Intersects(t.{t_geom_col_name}, j.{j_geom_col_name})
                        GROUP BY t.id
                    ) AS subquery
                    WHERE t.id = subquery.id;
                END $$;
            """)
            conn.execute(intersection_sql)
            conn.commit()
            logger.debug(f"Updated column {intersect_col_name} in table {target_table} with intersection results")

    except SQLAlchemyError as e:
        logger.error(f"Failed to run intersection for {target_table}: {e}")
        return False
    return True

def validate_geometry(table_name, geom_col, db_engine):
    """
    Validates and fixes the geometry column in the specified table.

    Args:
        table_name (str): Name of the table containing the geometry column.
        geom_col (str): Name of the geometry column to validate.
        db_engine (Engine): SQLAlchemy database engine.

    Returns:
        bool: True if all geometries are valid or successfully fixed, False otherwise.
    """
    try:
        with db_engine.connect() as connection:
            # Attempt to fix invalid geometries
            fix_invalid_sql = f"""
            UPDATE {table_name}
            SET {geom_col} = ST_MakeValid({geom_col})
            WHERE NOT ST_IsValid({geom_col});
            """
            connection.execute(text(fix_invalid_sql))

            # Re-check for invalid geometries
            find_invalid_sql = f"""
            SELECT {geom_col}
            FROM {table_name}
            WHERE NOT ST_IsValid({geom_col});
            """
            result = connection.execute(text(find_invalid_sql))
            invalid_geometries = result.fetchall()

            if invalid_geometries:
                logger.error(f"Failed to fix all invalid geometries in table {table_name}.")
                return False
            else:
                logger.info(f"All invalid geometries in table {table_name} have been successfully fixed.")
                return True
    except SQLAlchemyError as e:
        logger.error(f"Error validating geometries in table {table_name}: {e}")
        return False

def left_join_table(db_engine, join_column, original_table, joining_table, output_table, include_columns, exclude_columns):
    """
    Performs a left join between the original table and the joining table, including or excluding specified columns,
    and saves the result to a new table. If the output table is the same as the original table, it uses a temporary
    table to store the joined data and then replaces the original table with the temporary table.

    Args:
        db_engine (Engine): SQLAlchemy engine connected to the database.
        join_column (str): The column to join on.
        original_table (str): The name of the original table.
        joining_table (str): The name of the joining table.
        include_columns (list): List of columns to include from the joining table.
        exclude_columns (list): List of columns to exclude from the joining table.
        output_table (str): The name of the output table to save the joined data.

    Returns:
        bool: True if the join is successful and data is saved, False otherwise.
    """
    try:
        with db_engine.connect() as conn:
            # Get all columns from the original table
            original_columns_query = text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
            """)
            original_columns_result = conn.execute(original_columns_query, {"table_name": original_table})
            original_columns = [row[0] for row in original_columns_result]

            # Get all columns from the joining table
            joining_columns_query = text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
            """)
            joining_columns_result = conn.execute(joining_columns_query, {"table_name": joining_table})
            joining_columns = [row[0] for row in joining_columns_result]

            # Filter columns to include and exclude
            if include_columns:
                joining_columns = [col for col in joining_columns if col in include_columns]
            if exclude_columns:
                joining_columns = [col for col in joining_columns if col not in exclude_columns]

            # Remove duplicate columns
            joining_columns = [col for col in joining_columns if col not in original_columns]

            # Build the select clause
            select_clause = ", ".join([f"{original_table}.{col}" for col in original_columns] +
                                      [f"{joining_table}.{col}" for col in joining_columns])

            # Use a temporary table if the output table is the same as the original table
            temp_table = f"temp_joined_table_{str(getrandbits(16))}"
            if output_table == original_table:
                output_table = temp_table

            # Drop the temporary table if it already exists
            conn.execute(text(f"DROP TABLE IF EXISTS {output_table}"))

            # Build the join query
            join_query = text(f"""
                CREATE TABLE {output_table} AS
                SELECT {select_clause}
                FROM {original_table}
                LEFT JOIN {joining_table}
                ON {original_table}.{join_column} = {joining_table}.{join_column}
            """)

            # Execute the join query and save the result to the output table
            conn.execute(join_query)
            conn.commit()

            # Replace the original table with the temporary table if needed
            if output_table == temp_table:
                conn.execute(text(f"DROP TABLE {original_table}"))
                conn.execute(text(f"ALTER TABLE {temp_table} RENAME TO {original_table}"))
                conn.commit()

            logger.info(f"Joined data from {original_table} and {joining_table} on {join_column} saved to {output_table}")
            return True

    except SQLAlchemyError as e:
        logger.error(f"Failed to join tables {original_table} and {joining_table}: {e}")
        return False

def insert_data_into_new_table(db_engine, original_table, new_table, columns):
    """
    Inserts data from the original table into the new table for the specified columns.

    Args:
        db_engine (Engine): SQLAlchemy engine connected to the database.
        original_table (str): The name of the original table.
        new_table (str): The name of the new table.
        columns (list): List of column names to copy from the original table.

    Returns:
        bool: True if the data is inserted successfully, False otherwise.
    """
    try:
        with db_engine.connect() as conn:
            # Build the INSERT INTO ... SELECT statement
            columns_list = ", ".join(columns)
            insert_data_sql = f"""
            INSERT INTO {new_table} ({columns_list})
            SELECT {columns_list}
            FROM {original_table}
            """
            conn.execute(text(insert_data_sql))
            conn.commit()
            logger.debug(f"Copied data from {original_table} to {new_table} for columns: {columns_list}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"Failed to insert data into new table {new_table}: {e}")
        return False

def copy_table(db_engine, new_table, original_table, columns):
    """
    Copies the specified columns from the original table to the new table.
    Args:
        db_engine (Engine): SQLAlchemy engine connected to the database.
        table (str): The name of the new table.
        original_table (str): The name of the original table.
        columns (dict): Dict of column names to copy from the original table and their data types in the new table.
        Returns:
        bool: True if the table is copied successfully, False otherwise.
    """
    # try:
    #     with db_engine.connect() as conn:
    #         drop_and_rebuild_table(db_engine, new_table, columns)
    #         columns_list = list(columns.keys())
    #         insert_data_into_new_table(db_engine, original_table, new_table, columns_list)  
    #         logger.info(f"Copied data from {original_table} to {new_table} for columns: {columns}")
    #         return True
    try:
        drop_and_rebuild_table(db_engine, new_table, columns)
        columns_list = list(columns.keys())
        insert_data_into_new_table(db_engine, original_table, new_table, columns_list)  
        logger.info(f"Copied data from {original_table} to {new_table}")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Failed to copy table {new_table}: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while copying table {new_table}: {e}")
        return False

def table_to_geopackage(db_engine, source_table_name, geopackage_file_path, geopackage_table_name, overwrite_geopackage=False):
    """
    Publishes a table to a GeoPackage file using ogr2ogr.

    Args:
        db_engine (Engine): SQLAlchemy engine connected to the database.
        source_table_name (str): Name of the source table.
        geopackage_file_name (str): Name of the GeoPackage file.
        geopackage_table_name (str): Name of the table in the GeoPackage.
        overwrite_geopackage (bool): Whether to overwrite the GeoPackage file.
        overwrite_table (bool): Whether to overwrite the table in the GeoPackage.
    """
    #gdal.UseExceptions()
    try:
        conn_string = (
            f'PG:dbname={db_engine.url.database} '
            f'host={db_engine.url.host} '
            f'port={db_engine.url.port} '
            f'user={db_engine.url.username} '
            f'password={db_engine.url.password}'
        )

        # Delete geopackage if overwrite_geopackage is True
        if overwrite_geopackage and os.path.exists(geopackage_file_path):
            os.remove(geopackage_file_path)

        # Use gdal.VectorTranslate to publish the table to the GeoPackage
        gdal.VectorTranslate(
            geopackage_file_path,
            conn_string,
            format='GPKG',
            layerName=geopackage_table_name,
            SQLStatement=f'SELECT * FROM {source_table_name}',
            accessMode='overwrite'  # Set the access mode
        )

        logger.info(f"Table {source_table_name} published to GeoPackage {geopackage_file_path} as {geopackage_table_name}.")
    except Exception as e:
        logger.error(f"Unexpected error during execution: {e}")
        raise


# def table_to_agol_feature_service(db_engine, source_table_name, feature_service_name, feature_service_description, feature_service_tags, feature_service_summary, feature_service_url, overwrite_table=True):
#     """
#     Publishes a table to an ArcGIS Online Feature Service.

#     Args:
#         db_engine (Engine): SQLAlchemy engine connected to the database.
#         source_table_name (str): Name of the source table.
#         feature_service_name (str): Name of the feature service.
#         feature_service_description (str): Description of the feature service.
#         feature_service_tags (str): Tags for the feature service.
#         feature_service_summary (str): Summary of the feature service.
#         feature_service_url (str): URL of the feature service.
#         overwrite_table (bool): Whether to overwrite the table in the feature service.
#     """
#     try:
#         # Read the table from the database
#         gdf = gpd.read_postgis(f"SELECT * FROM {source_table_name}", db_engine, geom_col='geometry')

#         # Convert GeoDataFrame to GeoJSON
#         geojson = gdf.to_json()

#         # Publish to ArcGIS Online Feature Service
#         headers = {
#             'Content-Type': 'application/json',
#             'Accept': 'application/json'
#         }
#         payload = {
#             'f': 'json',
#             'token': 'YOUR_ARCGIS_TOKEN',
#             'overwrite': overwrite_table,
#             'name': feature_service_name,
#             'description': feature_service_description,
#             'tags': feature_service_tags,
#             'snippet': feature_service_summary,
#             'text': geojson
#         }
#         response = requests.post(feature_service_url, headers=headers, data=json.dumps(payload))
#         response.raise_for_status()

#         logger.info(f"Table {source_table_name} published to ArcGIS Online Feature Service {feature_service_name}.")
#     except Exception as e:
#         logger.error(f"Failed to publish table {source_table_name} to ArcGIS Online Feature Service: {e}")
#         raise