import requests
import json
from pipelines.sqlite import SQLiteDB
import pandas as pd
import logging
logger = logging.getLogger("airflow.task")


class IHCApiClient:
    def __init__(self, base_url: str, api_key: str, db_file: str):
        self.base_url = base_url
        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json',
            'x-api-key': api_key
        }
        self.sqlite = SQLiteDB(db_file)

    # post customer_journeys
    def post_customer_journeys(self, start_datetime: str, end_datetime: str) -> dict:

        try:
            customer_journeys = self.sqlite.get_customer_journeys_serialization(
                start_datetime=start_datetime, end_datetime=end_datetime)
            # self.sqlite.get_redistribution_parameter_serialization()
            redistribution_parameter = {}
            # Insert Conversion Type ID here
            conv_type_id = 'all_eu_markets'
            api_url = "https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={conv_type_id}".format(
                conv_type_id=conv_type_id)
            body = {
                'customer_journeys': customer_journeys,
                'redistribution_parameter': redistribution_parameter
            }
            body_data = json.dumps(body)
            logger.info(f"Posting customer journeys to {body_data}")
            if body_data is None:
                raise ValueError("No data to post")

            response = requests.post(
                api_url,
                data=body_data,
                headers={
                    'Content-Type': 'application/json',
                    'x-api-key': self.api_key
                }
            )
            results = response.json()

            # TODO : remove this , test porpuse only
            # results = {'statusCode': 200, 'value': [{'conversion_id': 'd95d45442caec2cdfe2a5e94e0eb6210d5082347d3a5ee3bddf17a434f70d471', 'session_id': 'ba74c387f8a2f5ee3facddc70ab1fbb546d71aa9f0a8c46d5f41a5ea518a8d93', 'initializer': 1.0, 'holder': 0.5, 'closer': 0.0, 'ihc': 0.5}, {
            #     'conversion_id': 'd95d45442caec2cdfe2a5e94e0eb6210d5082347d3a5ee3bddf17a434f70d471', 'session_id': '510ce0f454c0d54d6767aa22d1f2710643bae56473107a36356fe1a4db8bc36b', 'initializer': 0.0, 'holder': 0.5, 'closer': 1.0, 'ihc': 0.5}], 'partialFailureErrors': []}

            # logger.info(f"Status Code: {results['statusCode']}")
            # logger.info(results['value'])
            # logger.info(f"results: {results}")

            if results['statusCode'] == 200:
                logger.info("Successfully posted customer journeys")
            elif results['statusCode'] == 206:
                logger.warning(
                    "StatusCode 206, The request has succeeded but there are partial errors")
                logger.warning(results['partialFailureErrors'])
            elif results['statusCode'] == 400:
                raise ValueError(
                    "StatusCode 400, Request Failure due to invalid input")
            elif results['statusCode'] == 406:
                raise ValueError(
                    "StatusCode 406, (in partial failures object) error in parsing customer journey")

            return results
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            raise e
        except requests.exceptions.RequestException as e:
            logger.error(f"Error retrieving data: {e}")
            raise e
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
            raise e
        except Exception as e:
            logger.error(f"Error posting customer journeys: {e}")
            raise e

    def put_attribution_customer_journey(self, response: dict) -> bool:
        '''Deserialize JSON response and save into attribution_customer_journey'''
        try:
            # empty table first
            # TODO: review this concept
            query = "delete from attribution_customer_journey;"
            self.sqlite.execute_dml(query)
            logger.info("attribution_customer_journey table emptied")

            # insert data
            self.sqlite.insert_data(
                table_name='attribution_customer_journey', data=response['value'])
            return True
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
            raise e
        except Exception as e:
            logger.error(f"Error deserializing data: {e}")
            raise e

    def check_api_availability(self,) -> bool:
        '''Validate the given URL'''
        try:
            response = requests.get(self.base_url)
            if response.status_code == 403:  # forbidden, missing authenicaiton
                return True
            else:
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Error validating URL {self.base_url}: {e}")
            return False
