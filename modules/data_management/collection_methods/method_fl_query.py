import logging
from modules.infrastructure.other_ops.arcgis_operations import ArcGISFeatureLayerQuery, ArcGISOAuth2
from modules.data_management.sql_utils.sql_ops import validate_geometry

logger = logging.getLogger(__name__)

class method_fl_query:
    """
    Class to run an ArcGIS Feature Layer query with the given parameters.

    Attributes:
        method_configs (dict): Configuration dictionary for the method.
        db_engine (Engine): SQLAlchemy engine connected to the database.
        batch_size (int): Number of records to fetch in each batch.
        max_simultaneous_requests (int): Maximum number of simultaneous requests.
    """
    def __init__(self, data_source, db_engine, data_sources_folder):
        """
        Initializes the method_fl_query class with the given configurations and database engine.

        Args:
            data_source (DataSource): DataSource instance containing the configuration.
            db_engine (Engine): SQLAlchemy engine connected to the database.
            data_sources_folder (str): Path to the folder for storing data if necessary.
        """
        self.name = data_source.name
        self.method_configs = data_source.source_config['method_configs']
        self.query_params = self.method_configs['query_params']
        self.query_params['f'] = 'geojson'
        if self.method_configs['client_id']:
            self.query_params['token'] = ArcGISOAuth2(self.method_configs['client_id']).token
        self.table_name = data_source.table_name
        self.table_columns = list(data_source.table_columns.keys())
        if 'id' in self.table_columns:
            self.table_columns.remove('id')
        self.db_engine = db_engine
        self.batch_size = 250
        self.max_simultaneous_requests = 10

        logger.debug(f"Initialized method_fl_query for data source: {self.name}")

    def collect_data(self):
        """
        Runs the ArcGIS Feature Layer query with the given parameters.

        Returns:
            bool: True if the query is successful, False otherwise.
        """
        logger.debug(f"Starting data collection for data source: {self.name}")
        try:
            arcgis_query = ArcGISFeatureLayerQuery(
                url=self.method_configs['query_url'], 
                query_params=self.query_params, 
                table_name=self.table_name,
                table_columns=self.table_columns,
                db_engine=self.db_engine,
                batch_size=self.batch_size,
                max_simultaneous_requests=self.max_simultaneous_requests
            )
            success = arcgis_query.fetch_data()
            #logger.debug(f"Data collection successful for data source: {self.name}")

            # Validate geometry after data collection
            # is_geometry_valid =  validate_geometry(self.table_name, 'geometry', self.db_engine)
            # return is_geometry_valid
            return success

        except Exception as e:
            logger.error(f"Failed to run ArcGIS Feature Layer query for data source {self.name}: {e}")
            return False