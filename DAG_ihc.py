
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.models import Variable
from pipelines.sqlite import SQLiteDB
from pipelines.ihc_api import IHCApiClient
from pipelines.upload import S3Uploader
from pathlib import Path
import json
import logging
task_logger = logging.getLogger("airflow.task")


# get env variables
db_path = Variable.get(
    "IHC_DB_PATH", default_var="db_dir/challenge.db")
api_url = Variable.get(
    "IHC_API_URL")
api_key = Variable.get("IHC_API_KEY")
last_processed_datetime = Variable.get(
    "IHC_LAST_PROCESSED_DATETIME", default_var='2023-08-29 08:00:02.100000')
chunk_size_hour = Variable.get("IHC_CHUNK_SIZE_HOUR", default_var=12)
aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
aws_bucket_name = Variable.get("AWS_BUCKET_NAME")
aws_bucket_path = Variable.get("AWS_BUCKET_PATH")
aws_region_name = Variable.get("AWS_REGION_NAME")

# set class variables
project_root_path = Path(__file__).resolve().parent
db = SQLiteDB(f"{project_root_path}/{db_path}")
ihc = IHCApiClient(base_url=api_url,
                   api_key=api_key,
                   db_file=f"{project_root_path}/{db_path}")
s3 = S3Uploader(aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key, bucket_name=aws_bucket_name, region_name=aws_region_name)


# Default settings applied to all tasks
default_args = {
    "owner": "ihc",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": "ebrahim.520@gmail.com",
    "email_on_retry": False,
    "retries": 0
}


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ihc"],
    default_args=default_args
)
def Dag_ihc():
    @task
    def start_dag():
        task_logger.info("check env variables if exists")
        if not api_key:
            raise ValueError("api_key is not set")
        #
        task_logger.info(f"last_processed_datetime: {last_processed_datetime}")
        task_logger.info(f"api_url is: {api_url}")

    # Start task group definition
    @task_group(group_id='sqlite_task_group', default_args={"conn_id": "postgres_default"})
    def sqlite_prep_ddl():
        @task
        def validate_connection() -> bool:
            task_logger.info("validate connection")
            # double check with query
            db.get_tables_list()
            return True

        @task(email="ebrahim.520@gmail.com", email_on_failure=True)
        def create_table_attribution_customer_journey() -> bool:
            task_logger.info("creating table attribution_customer_journey")
            db.create_attribution_customer_journey_table()
            task_logger.info("created table attribution_customer_journey")
            return True

        @task
        def create_table_channel_reporting():
            task_logger.info("creating table channel_reporting")
            db.create_channel_reporting_table()
            task_logger.info("created table channel_reporting")
            return True

        chain(validate_connection(), create_table_attribution_customer_journey(
        ), create_table_channel_reporting())
    # End task group definition

    @task_group()
    def post_customer_journey_task_group(lp_datetime_tg: datetime = None, chunk_size_hour_tg: int = None):

        @task
        def validate_api_connection():
            task_logger.info("validate_api_connection")
            res = ihc.check_api_availability()
            if res:
                task_logger.info("API is available")
            else:
                task_logger.error("API is not available")

        @task
        def split_datetime_ranges(lp_datetime_t: datetime, chunk_size_hour_t: int):
            task_logger.info("split_datetime_ranges")
            # get last processed datetime from input params
            lp_datetime = datetime.strptime(
                lp_datetime_t, "%Y-%m-%d %H:%M:%S.%f")
            lp_datetime = lp_datetime.replace(microsecond=0)
            # get events date time min max
            db_start_datetime, db_end_datetime = db.get_event_datetimes_range()
            task_logger.info(f"last processed datetime: {
                             lp_datetime}, end date from database: {db_end_datetime}")
            if lp_datetime is None:
                lp_datetime = db_start_datetime

            #
            if type(lp_datetime) == str or type(db_end_datetime) == str:
                task_logger.info(f"lp_datetime: {lp_datetime}, db_end_datetime: {
                                 db_end_datetime} , chunk_size_hour_t: {chunk_size_hour_t}")
                raise ValueError("datetime is not in datetime format")
            # split datetime ranges
            time_ranges = db.split_datetime_ranges(
                lp_datetime, db_end_datetime, int(chunk_size_hour_t))

            return time_ranges

        @task(email="ebrahim.520@gmail.com", email_on_failure=True)
        def post_customer_journey(time_ranges: list):
            task_logger.info("post_customer_journey...")

            for start, end in time_ranges:
                print(f"Start: {start}, End: {end}")
                # post customer journeys
                response = ihc.post_customer_journeys(
                    start_datetime=start, end_datetime=end)
                # TODO : will replace
                break
            # update last processed datetime
            Variable.set("IHC_LAST_PROCESSED_DATETIME", end)
            return response

        @task
        def sink_attribution_customer_journey(response: json):
            task_logger.info(
                "insert recieved result into attribution_customer_journey...")
            result = ihc.put_attribution_customer_journey(response=response)
            if result:
                task_logger.info(
                    "insert into attribution_customer_journey was successful")
                return True
            else:
                task_logger.error(
                    "insert into attribution_customer_journey was not successful")
                return False

        @task
        def merge_channel_reporting():
            task_logger.info("merge channel_reporting...")
            db.put_channel_reporting()
            task_logger.info("merge channel_reporting was successful")

        chain(validate_api_connection(), sink_attribution_customer_journey(post_customer_journey(split_datetime_ranges(lp_datetime_t=lp_datetime_tg, chunk_size_hour_t=chunk_size_hour_tg
                                                                                                                       ))), merge_channel_reporting())
    # End task group definition

    @task
    def get_channel_reporting_csv():
        task_logger.info("get_channel_reporting_csv...")
        task_logger.info(f"project dir: {project_root_path}")
        db.get_channel_reporting_csv(
            sink_path='/usr/local/airflow/dags/sink_dir')

    @task
    def copy_report_to_cloud(bucket_path: str):
        task_logger.info("copy_report_to_s3...")
        s3.upload_csv(sink_path='/usr/local/airflow/dags/sink_dir', local_fullname='channel_reporting.csv',
                      cloud_file_path=bucket_path, cloud_filename='channel_reporting')
        task_logger.info("copy_report_to_s3 was successful")

    @task
    def finish_dag():
        task_logger.info("finish...")

    # Set task group's (tg1) dependencies
    chain(start_dag(), sqlite_prep_ddl(), post_customer_journey_task_group(
        lp_datetime_tg=last_processed_datetime, chunk_size_hour_tg=chunk_size_hour),
        get_channel_reporting_csv(), copy_report_to_cloud(bucket_path=aws_bucket_path), finish_dag())


# Instantiate the DAG
Dag_ihc()
