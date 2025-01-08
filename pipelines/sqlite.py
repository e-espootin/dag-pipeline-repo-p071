from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json
# from utils.logger import setup_logger
import os
from datetime import datetime
import numpy as np
from typing import Tuple
import logging
logger = logging.getLogger("airflow.task")


class SQLiteDB:
    def __init__(self, db_file):
        """Initialize the SQLiteDB class with a database file."""
        self.db_file = db_file
        self.conn = self.create_connection()
        self.engine_url = f'sqlite:///{self.db_file}'
        self.engine = None

    def create_connection(self):
        """Create a database connection to the SQLite database."""
        conn = None
        try:
            if self.check_file_exists():
                self.engine = create_engine(
                    f'sqlite:///{self.db_file}', echo=True)
                conn = self.engine.connect()
                logger.info(f"Connected to SQLite database: {self.db_file}")
                # Use Inspector to get table names
                inspector = inspect(self.engine)
                tables = inspector.get_table_names()
                logger.info(f"Tables in database: {tables}")
                # get tables list
                # self.get_tables_list()
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise e
        return conn

    def check_file_exists(self) -> bool:
        '''Check if the database file exists'''
        try:
            if os.path.exists(self.db_file):
                logger.info(f"Database file exists: {self.db_file}")
                return True
            else:
                logger.critical(
                    f"Database file does not exist: {self.db_file}")
                logger.warning(f"fyi, need to check {self.db_file}")
                return False
        except Exception as e:
            logger.error(f"Error checking if file exists: {e}")
            raise e

    def get_tables_list(self):
        '''Get list of tables in the database'''
        try:
            query_string = "SELECT name FROM sqlite_master WHERE type='table';"
            # query_string_db_info = "PRAGMA database_list;"
            self.engine = create_engine(self.engine_url, echo=True)
            Session = sessionmaker(bind=self.engine)
            with Session() as session:  # Use a context manager for proper session handling
                try:
                    result = session.execute(text(query_string))
                    tables = result.fetchall()  # Fetch all results
                    logger.info(f"list tables in database: {tables}")
                except Exception as e:
                    logger.error(f"Error executing query: {e}")
                    raise e
        except Exception as e:
            logger.error(f"Error getting tables list: {e}")
            raise e

    # def create_table(self, create_table_sql):
    #     """Create a table from the create_table_sql statement."""
    #     try:
    #         self.conn.execute(create_table_sql)
    #         logger.info("Table created successfully")
    #     except Exception as e:
    #         logger.error(f"Error creating table: {e}")

    def create_attribution_customer_journey_table(self):
        """Create the attribution_customer_journey table."""
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS attribution_customer_journey (
                                        conv_id text NOT NULL,
                                        session_id text NOT NULL,
                                        ihc real NOT NULL,
                                        PRIMARY KEY(conv_id,session_id)
                                    );
            """

            if self.conn is not None:
                # Execute the table creation query
                self.conn.execute(text(create_table_sql))
            else:
                logger.error(
                    "Error: Unable to establish a database connection.")

            logger.info(
                "Table attribution_customer_journey created successfully")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise e

    def create_channel_reporting_table(self):
        """Create the channel_reporting table."""
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS channel_reporting (
                                channel_name text NOT NULL,
                                date text NOT NULL,
                                cost real NOT NULL,
                                ihc real NOT NULL,
                                ihc_revenue real NOT NULL,
                                PRIMARY KEY(channel_name,date)
                            );
            """

            if self.conn is not None:
                # Execute the table creation query
                self.conn.execute(text(create_table_sql))
            else:
                logger.error(
                    "Error: Unable to establish a database connection.")

            logger.info("Table channel_reporting created successfully")

        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise e

    def read_data(self, query):
        """Read data from the database using the provided query."""
        try:
            # Execute the query and load the result into a DataFrame
            self.engine = create_engine(self.engine_url, echo=True)
            self.engine = create_engine(
                f'sqlite:///{self.db_file}', echo=True)
            Session = sessionmaker(bind=self.engine)
            with Session() as session:  # Use a context manager for proper session handling
                try:
                    result = session.execute(text(query))
                    tables = result.mappings().all()
                    return tables
                except Exception as e:
                    # session.rollback()  # Crucial to prevent data corruption on errors
                    logger.error(f"Error executing query: {e}")
        except Exception as e:
            logger.error(f"Error reading data: {e}")
            raise e

    def get_customer_journeys_serialization(self, start_datetime: str, end_datetime: str) -> json.dumps:
        '''customer_journeys: list of sessions making up customer journeys'''
        try:
            query = f"""
            WITH cte_0 AS (
                select ss.user_id
                    , ss.session_id
                    , DATETIME(ss.event_date || ' ' || ss.event_time) AS session_event_timestamp
                    , ss.channel_name
                    , ss.holder_engagement
                    , ss.closer_engagement
                    , case when DATETIME(ss.event_date || ' ' || ss.event_time) < DATETIME(c.conv_date || ' ' || c.conv_time) then 1 else 0 end as during_session_can
                    , c.conv_id
                    , DATETIME(c.conv_date || ' ' || c.conv_time) AS conversion_event_timestamp
                from session_sources ss
                inner join  conversions c
                on c.user_id = ss.user_id
                where DATETIME(ss.event_date || ' ' || ss.event_time) between '{start_datetime}' and '{end_datetime}'
            ), cte_1 AS(
            select *
                , ROW_NUMBER() OVER(PARTITION by c.user_id, c.conv_id ORDER by c.during_session_can desc, c.session_event_timestamp desc) as rn
            from cte_0 as c)
            SELECT c.conv_id as conversion_id
                , c.session_id
                , c.session_event_timestamp AS timestamp
                , c.channel_name as channel_label
                , c.holder_engagement
                , c.closer_engagement
                , case when rn = 1 then 1 else 0 end conversion
                --, 0 as impression_interaction
            FROM  cte_1 as c;
            """
            output = self.read_data(query)
            # Convert to list of dictionaries
            rows = [dict(row) for row in output]

            # df = pd.DataFrame(rows)
            # df.to_csv('customer_journeys.csv')
            return rows
            # data serialization
            # return json.dumps(rows)
        except Exception as e:
            print(
                f"get_customer_journeys_serialization: Error reading data: {e}")
            raise e

    def get_redistribution_parameter_serialization(self):
        '''redistribution_parameter: dictionary of redistribution parameters defining how channel results are “redistributed” to other channels (e.g. direct channel attribution)'''
        try:
            query = f"""
            select 1 {None}
            """
            output = self.read_data(query)
            # Convert to list of dictionaries
            rows = [dict(row) for row in output]
            # data serialization
            return json.dumps(rows)
        except Exception as e:
            print(
                f"get_redistribution_parameter_serialization: Error reading data: {e}")
            raise e

    def get_event_datetimes_range(self) -> Tuple[datetime, datetime]:
        """Get the range of event datetimes from the session_sources table."""
        try:
            query = """
            SELECT 
                MIN(DATETIME(event_date || ' ' || event_time)) AS start_datetime,
                MAX(DATETIME(event_date || ' ' || event_time)) AS end_datetime
            FROM session_sources;
            """
            output = self.read_data(query)
            if output:
                start_datetime = output[0]['start_datetime']
                end_datetime = output[0]['end_datetime']
                logger.info(f"Event datetimes range: {
                            start_datetime} to {end_datetime}")
                return datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S"), datetime.strptime(end_datetime, "%Y-%m-%d %H:%M:%S")
            else:
                logger.warning(
                    "No event datetimes found in the session_sources table.")
                return None, None
        except Exception as e:
            logger.error(f"Error getting event datetimes range: {e}")
            raise e

    def split_datetime_ranges(self, start_datetime: datetime, end_datetime: datetime, x_hours: int) -> list:
        '''Split the datetime range into intervals of x_hours'''
        total_seconds = (end_datetime - start_datetime).total_seconds()
        intervals = int(total_seconds // (x_hours * 3600)) + 1

        timestamps = np.linspace(
            start_datetime.timestamp(),
            end_datetime.timestamp(),
            num=intervals
        )

        # Convert timestamps back to datetime objects
        time_ranges = [(datetime.fromtimestamp(timestamps[i]),
                        datetime.fromtimestamp(timestamps[i+1]))
                       for i in range(len(timestamps)-1)]

        return time_ranges

    def insert_data(self, table_name: str, data: dict):
        """Insert data into the specified table."""
        try:
            if not data:
                logger.warning("No data provided for insertion.")
                return

            self.engine = create_engine(self.engine_url, echo=True)
            Session = sessionmaker(bind=self.engine)
            with Session() as session:
                try:
                    session.execute(
                        text(f"INSERT INTO {table_name} (conv_id, session_id, ihc) VALUES (:conversion_id, :session_id, :ihc)"), data)
                    session.commit()
                    logger.info(f"Data inserted into {
                                table_name} successfully.")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Error inserting data into {
                                 table_name}: {e}")
                    raise e
        except Exception as e:
            logger.error(f"Error in insert_data method: {e}")
            raise e

    def execute_dml(self, query):
        """Execute an insert statement with the provided query and parameters."""
        try:
            self.engine = create_engine(self.engine_url, echo=True)
            Session = sessionmaker(bind=self.engine)
            with Session() as session:
                try:
                    session.execute(text(query))
                    session.commit()
                    logger.info("Insert statement executed successfully.")
                except Exception as e:
                    session.rollback()
                    raise e
        except Exception as e:
            logger.error(f"Error in execute_insert method: {e}")
            raise e

    def put_channel_reporting(self):
        '''Insert channel_reporting data into the database'''
        try:
            # make table empty
            # # TODO: review this concept
            # query = "delete from channel_reporting;"
            # self.execute_dml(query)
            # logger.info("channel_reporting table emptied")

            # insert data
            query = """
                insert into channel_reporting (channel_name, date, cost, ihc, ihc_revenue)
            select ss.channel_name 
                , ss.event_date  as date
                , sum(IFNULL(sc.cost, 1)) as cost 
                , avg(acj.ihc) as ihc
                , sum(IFNULL(sc.cost, 0) * acj.ihc) as ihc_revenue
            FROM attribution_customer_journey acj 
            inner join conversions c 
                on c.conv_id  = acj.conv_id 
            inner join session_sources ss 
                on ss.session_id  = acj.session_id and ss.user_id  = c.user_id 
            INNER JOIN session_costs sc 
                on sc.session_id  = acj.session_id
            where not exists(select 1 from channel_reporting as z where z.channel_name = ss.channel_name  and z.date = ss.event_date)
            group by ss.channel_name, ss.event_date;
            """
            self.execute_dml(query)
        except Exception as e:
            print(f"Error in put_channel_reporting method: {e}")
            raise e

    def get_channel_reporting_csv(self, sink_path: str):
        '''Get channel_reporting data from the database and save it to a CSV file'''
        try:
            query = """
            select cr.channel_name 
                , cr.date 
                , cr.cost 
                , cr.ihc 
                , cr.ihc_revenue 
                -- CPO: (cost per order) showing the amount of marketing costs for the given date and channel that was spent on getting one attributed (IHC) order
                , cost / IFNULL(ihc, 1) as CPO
                -- ROAS: (return on ad spend) showing revenue earned for each Euro you spend on marketing]
                , ihc_revenue / IFNULL(cost, 1) as ROAS
            from channel_reporting cr ;
            """
            output = self.read_data(query)
            df = pd.DataFrame(output)
            destination_path = f"{sink_path}/channel_reporting.csv"
            logger.info(f"Saving channel_reporting reporting file into {
                        destination_path}")
            df.to_csv(destination_path, index=False)
            logger.info(
                "channel_reporting data saved to channel_reporting.csv")
        except Exception as e:
            logger.error(f"Error getting channel_reporting data: {e}")
            raise e
