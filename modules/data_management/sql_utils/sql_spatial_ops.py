import logging

logger = logging.getLogger(__name__)

def create_spatial_index(engine, table_name, geom_column):
    """
    Creates a spatial index on the specified geometry column of a table.

    Args:
        engine (Engine): SQLAlchemy engine connected to the database.
        table_name (str): The name of the table.
        geom_column (str): The name of the geometry column to index.

    Returns:
        None
    """
    if 'sqlite' in str(engine.url):
        with engine.connect() as conn:
            conn.execute(f"SELECT CreateSpatialIndex('{table_name}', '{geom_column}')")
            logger.debug(f"Spatial index created on {geom_column} in table {table_name}")
    elif 'postgresql' in str(engine.url):
        logger.error("PostGIS support is not yet implemented.")
    else:
        logger.error("Unsupported SQL backend.")

def update_points_with_intersection(engine, points_table, polygons_table, points_geom_column='geometry', polygons_geom_column='geometry'):
    """
    Updates the points table with a new column indicating intersection with polygons.

    Args:
        engine (Engine): SQLAlchemy engine connected to the database.
        points_table (str): The name of the points table.
        polygons_table (str): The name of the polygons table.
        points_geom_column (str): The geometry column in the points table.
        polygons_geom_column (str): The geometry column in the polygons table.

    Returns:
        None
    """
    if 'sqlite' in str(engine.url):
        with engine.connect() as conn:
            try:
                # Add a new column to the points table if it doesn't exist
                conn.execute(f'ALTER TABLE {points_table} ADD COLUMN IF NOT EXISTS intersects BOOLEAN')

                # Update the points table with intersection results
                conn.execute(f'''
                    UPDATE {points_table}
                    SET intersects = (
                        SELECT EXISTS (
                            SELECT 1
                            FROM {polygons_table}
                            WHERE ST_Intersects({points_table}.{points_geom_column}, {polygons_table}.{polygons_geom_column})
                        )
                    )
                ''')
                logger.debug(f"Points table {points_table} updated with intersection results")
            except Exception as e:
                logger.error(f"Failed to update points table {points_table} with intersection results: {e}")
                raise
    elif 'postgresql' in str(engine.url):
        logger.error("PostGIS support is not yet implemented.")
    else:
        logger.error("Unsupported SQL backend.")