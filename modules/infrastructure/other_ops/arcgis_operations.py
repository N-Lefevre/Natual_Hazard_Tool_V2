import logging
import requests
import time
import json
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, as_completed
from modules.data_management.sql_utils.sql_ops import clear_table, geojson_to_postgis
from arcgis.gis import GIS

logger = logging.getLogger(__name__)

class ArcGISOAuth2:
    """
    Class to handle OAuth2 authentication with ArcGIS Online and provide an updated token.

    Attributes:
        client_id (str): Client ID of the OAuth2 application.

    """
    def __init__(self, client_id):
        """
        Initializes the ArcGISOAuth2 class with the given client ID.

        Args:
            client_id (str): Client ID of the OAuth2 application.
        """
        self.client_id = client_id
        self._token = None

    @property
    def token(self):
        """
        Authenticates with ArcGIS Online and returns an updated access token.

        Returns:
            str: Updated access token.
        """
        try:
            gis = GIS("https://www.arcgis.com", client_id=self.client_id)
            self._token = gis._con.token
            logger.info("Successfully obtained new access token.")
            return self._token
        except Exception as e:
            logger.error(f"Failed to obtain access token: {e}")
            raise

class ArcGISFeatureLayerQuery:
    """
    Class to fetch data from an ArcGIS Feature Layer query and save it to a PostGIS database.

    Attributes:
        query_url (str): URL for the ArcGIS Feature Layer query.
        query_params (dict): Query parameters for the request.
        table_name (str): Name of the table to save the data in the database.
        db_engine (Engine): SQLAlchemy engine connected to the database.
        batch_size (int): Number of records to fetch in each batch.
        max_simultaneous_requests (int): Maximum number of simultaneous requests.
    """
    def __init__(self, url, query_params, table_name, table_columns, db_engine, batch_size=1000, max_simultaneous_requests=5):
        """
        Initializes the ArcGISFeatureLayerQuery class with the given configurations and database engine.

        Args:
            url (str): URL for the ArcGIS Feature Layer query.
            query_params (dict): Query parameters for the request.
            table_name (str): Name of the table to save the data in the database.
            db_engine (Engine): SQLAlchemy engine connected to the database.
            batch_size (int): Number of records to fetch in each batch.
            max_simultaneous_requests (int): Maximum number of simultaneous requests.
        """
        self.query_url = url
        self.query_params = query_params
        self.table_name = table_name
        self.table_columns = table_columns
        self.db_engine = db_engine
        self.batch_size = batch_size
        self.max_simultaneous_requests = max_simultaneous_requests
        self.session = requests.Session()
        self.total_features = 0
        self.total_expected_features = 0

    # def fetch_data(self):
    #     """
    #     Fetches data from the ArcGIS Feature Layer in batches and saves it to the database.

    #     This method handles fetching data in batches, making multiple simultaneous requests,
    #     and saving each batch to the database. It continues fetching data until no more data is available.
    #     """
    #     # Get the total number of features
    #     self.total_expected_features = self._get_total_feature_count()
    #     if self.total_expected_features == 0:
    #         logger.debug("No features to fetch.")
    #         return

    #     logger.debug(f"Total expected features: {self.total_expected_features}")

    #     offset = 0
    #     clear_table(self.db_engine, self.table_name)
    #     batch_number = 1
    #     total_batches = (self.total_expected_features + self.batch_size - 1) // self.batch_size

    #     while offset < self.total_expected_features:
    #         # Use ThreadPoolExecutor to handle multiple simultaneous requests
    #         with ThreadPoolExecutor(max_workers=self.max_simultaneous_requests) as executor:
    #             futures = []
    #             for i in range(self.max_simultaneous_requests):
    #                 current_offset = offset + i * self.batch_size
    #                 if current_offset >= self.total_expected_features:
    #                     break
    #                 # Prepare query parameters for the current batch
    #                 params = self.query_params.copy()
    #                 params['resultOffset'] = current_offset
    #                 params['resultRecordCount'] = self.batch_size
    #                 futures.append(executor.submit(self._fetch_batch, params))

    #             for future in as_completed(futures):
    #                 try:
    #                     data = future.result()
    #                     # if not data:
    #                     #     logger.debug(f"Total features collected: {self.total_features}")
    #                     #     return
    #                     if not data:
    #                         if batch_number < total_batches:
    #                             logger.error(f"No data received for batch {batch_number}/{total_batches}.")
    #                             #raise Exception(f"Batch number {batch_number} failed to receive data.")
    #                         else:
    #                             logger.debug(f"Total features collected: {self.total_features}")
    #                             return
    #                     self.total_features += len(data)
    #                     #logger.debug(f"Received {len(data)} features from the request")
    #                     self._save_to_db(data)
    #                     logger.debug(f"Batch {batch_number}/{total_batches} processed. Total features received so far: {self.total_features}/{self.total_expected_features}")
    #                     batch_number += 1
    #                 except Exception as e:
    #                     logger.error(f"Failed to fetch or save data: {e}")

    #         # Increment offset for the next set of batches
    #         offset += self.batch_size * self.max_simultaneous_requests
    #         #logger.debug(f"Next offset: {offset}")

    def fetch_data(self):
        """
        Fetches data from the ArcGIS Feature Layer in batches and saves it to the database.

        This method handles fetching data in batches, making multiple simultaneous requests,
        and saving each batch to the database. It continues fetching data until no more data is available.

        Returns:
            bool: True if the data collection was successful, False otherwise.
        """
        # Get the total number of features
        self.total_expected_features = self._get_total_feature_count()
        if self.total_expected_features == 0:
            logger.debug("No features to fetch.")
            return

        logger.debug(f"Total expected features: {self.total_expected_features}")

        offset = 0
        clear_table(self.db_engine, self.table_name)
        batch_number = 1
        total_batches = (self.total_expected_features + self.batch_size - 1) // self.batch_size

        logger.info("Progress: 0% complete.")

        start_time = time.time()  # Start the timer for progress logging
        last_log_time = start_time  # Track the last time progress was logged

        while offset < self.total_expected_features:
            # Use ThreadPoolExecutor to handle multiple simultaneous requests
            with ThreadPoolExecutor(max_workers=self.max_simultaneous_requests) as executor:
                futures = []
                for i in range(self.max_simultaneous_requests):
                    current_offset = offset + i * self.batch_size
                    if current_offset >= self.total_expected_features:
                        break
                    # Prepare query parameters for the current batch
                    params = self.query_params.copy()
                    params['resultOffset'] = current_offset
                    params['resultRecordCount'] = self.batch_size
                    futures.append(executor.submit(self._fetch_batch, params))

                for future in as_completed(futures):
                    try:
                        data = future.result()
                        if not data:
                            logger.error(f"No data received for batch {batch_number}/{total_batches}.")
                            logger.info(f"Collection failed at {progress_percentage:.1f}%")
                            logger.debug(f"Total features collected: {self.total_features}")
                            return False

                        self.total_features += len(data)
                        self._save_to_db(data)
                        logger.debug(f"Batch {batch_number}/{total_batches} processed. Total features received so far: {self.total_features}/{self.total_expected_features}")
                        batch_number += 1

                        # Log progress every 10 seconds
                        current_time = time.time()
                        progress_percentage = (self.total_features / self.total_expected_features) * 100
                        if current_time - last_log_time >= 10:
                            logger.info(f"Progress: {progress_percentage:.1f}% complete.")
                            last_log_time = current_time

                    except Exception as e:
                        logger.error(f"Failed to fetch or save data: {e}")
                        return False

            # Increment offset for the next set of batches
            offset += self.batch_size * self.max_simultaneous_requests

        logger.debug("Featurelayer data collection completed successfully.")
        logger.debug(f"Total features collected: {self.total_features}/{self.total_expected_features}")
        logger.info(f"Progress: {progress_percentage:.1f}% complete.")
        return True
        
    def _get_total_feature_count(self):
        """
        Fetches the total number of features available from the ArcGIS Feature Layer.

        Returns:
            int: Total number of features.
        """
        params = self.query_params.copy()
        params['returnCountOnly'] = True
        try:
            response = self.session.post(self.query_url, data=params)
            response.raise_for_status()
            if response.content:
                response_json = response.json()
                count = response_json.get('properties', {}).get('count', 0)
                return count
            else:
                logger.warning("Empty response received when fetching total feature count.")
        except requests.RequestException as e:
            logger.warning(f"Request failed when fetching total feature count: {e}")
        except ValueError as e:
            logger.warning(f"Failed to parse JSON response when fetching total feature count: {e}")
        return 0

    def _fetch_batch(self, params):
        """
        Fetches a single batch of data from the ArcGIS Feature Layer.

        This method includes retry logic to handle failed requests.

        Args:
            params (dict): Query parameters for the request.

        Returns:
            list: List of features fetched from the ArcGIS Feature Layer.
        """
        for attempt in range(3):
            try:
                response = self.session.post(self.query_url, data=params)
                response.raise_for_status()
                if response.content:
                    response_json = response.json()
                    features = response_json.get('features', [])
                    if features:
                        logger.debug(f"Received {len(features)} features in response")
                        return features
                    else:
                        logger.warning(f"No features found in response (attempt {attempt + 1}/3)")
                else:
                    logger.warning(f"Empty response received (attempt {attempt + 1}/3)")
            except requests.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/3): {e}")
            except ValueError as e:
                logger.warning(f"Failed to parse JSON response (attempt {attempt + 1}/3): {e}")
            time.sleep(10)
        logger.error(f"Failed to fetch data after 3 attempts for params: {params}")
        return []

    def _save_to_db(self, data):
        """
        Saves a batch of data to the PostGIS database using raw SQL.

        Args:
            data (list): List of features to save to the database.
        """
        if not data:
            return

        geojson_to_postgis(self.db_engine, self.table_name, self.table_columns, features=data)
