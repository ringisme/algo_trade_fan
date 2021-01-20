"""
This script is to initialize the AWS RDS PostgreSQL database,
as well as store modification functions to manage the database
"""

import pandas as pd
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Column, String, Float, DateTime, Integer, BigInteger, Date
from sqlalchemy import select, func, delete, Table, distinct
from etl_utils.etl_config import RDS_CONFIG


class RemoteDatabase:
    """
    This class is to manage the connection of AWS RDS PostgreSQL database
    """

    def __init__(self, tb_name, user_name, password, endpoint, db_name=RDS_CONFIG["DATABASE"]):
        self.tb_name = tb_name
        self.user_name = user_name
        self.password = password
        self.endpoint = endpoint
        self.db_name = db_name
        self.engine = create_engine('postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(user_name,
                                                                                       password,
                                                                                       endpoint,
                                                                                       RDS_CONFIG["PORT"],
                                                                                       db_name),
                                    pool_pre_ping=True, pool_size=10, max_overflow=2, pool_recycle=300,
                                    pool_use_lifo=True)
        self.metadata = MetaData(self.engine)
        # check if the table exists:
        if self.tb_name in self.engine.table_names():
            # Load the table:
            self.current_table = Table(self.tb_name, self.metadata, autoload=True)
        # if no such table, ask to create:
        else:
            print('The accessing table do not exists')
            build_tb = input('input "yes" to create [{}]: '.format(self.tb_name))
            if build_tb == "yes":
                self.create_table()
            else:
                raise Exception("You have to create a table before access.")

    def __enter__(self):
        self.connection = self.engine.connect()
        # build the connection to bulk insert data.
        self.up_con = psycopg2.connect(database=self.db_name,
                                       user=self.user_name,
                                       password=self.password,
                                       host=self.endpoint,
                                       port=RDS_CONFIG["PORT"])
        try:
            self.stack_list = self.stack_list()['symbol'].values.tolist()
        except:
            print("No stack found in database.")
            self.stack_list = list()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
        self.up_con.close()

    def stack_list(self):
        """
        This method is to pre-load the existed stack list for checking new stacks.

        :return: (DataFrame)
        """
        print("Preparing Stack List ... ")
        # Create statement
        stmt = select([distinct(self.current_table.columns.symbol)])
        # Fetch the data
        result = self.connection.execute(stmt).fetchall()
        result = pd.DataFrame(result)
        if not result.empty:
            result.columns = ['symbol']
        return result

    def create_table(self):
        """
        Build three main tables to save 'daily' candles, 'intraday minute level' candles, and 'splits' information.
        """
        self.metadata = MetaData(self.engine)
        if self.tb_name in self.engine.table_names():
            print("The table [{}] has already been created, please drop it at first.".format(self.tb_name))
            return None
        else:
            print("----- CREATING TABLE [{}] -----".format(self.tb_name))
        # Check if the table are pre-defined table:
        if self.tb_name in [RDS_CONFIG['DAILY_TABLE'], RDS_CONFIG['INTRADAY_TABLE']]:
            self.current_table = Table(self.tb_name, self.metadata,
                                       Column('close_price', Float(5)),
                                       Column('high_price', Float(5)),
                                       Column('low_price', Float(5)),
                                       Column('open_price', Float(5)),
                                       Column('status', String(255)),
                                       Column('timestamp', DateTime(timezone=True),
                                              default=datetime.utcnow, primary_key=True),
                                       Column('volume', BigInteger()),
                                       Column('symbol', String(255), primary_key=True))
        elif self.tb_name == RDS_CONFIG['SPLIT_TABLE']:
            self.current_table = Table(self.tb_name, self.metadata,
                                       Column('symbol', String(255), primary_key=True),
                                       Column('date', Date(), primary_key=True),
                                       Column('fromFactor', Integer()),
                                       Column('toFactor', Integer()),
                                       Column('source', String(255))
                                       )
        else:
            print("An empty table [{}] wil be created.".format(self.tb_name))
            # To create tables, it must contains at least one columns.
            self.current_table = Table(self.tb_name, self.metadata,
                                       Column('col', String(255), primary_key=True))
        # Build the table:
        self.metadata.create_all(self.engine)
        # To check if the table is successful created:
        print("----- TABLE [{}] CREATED -----".format(self.tb_name))

    def info(self):
        print("----- COLUMNS INFO -----")
        print(self.current_table.columns.keys())

    def drop_table(self):
        print("----- DROP TABLE [{}] -----".format(self.tb_name))
        self.current_table.drop(self.engine)
        print("----- TABLE [{}] DOPED -----".format(self.tb_name))

    def update_dataframe(self, df):
        """
        :param df: (DataFrame)
        :return: None. Only process operations in database.
        """
        if df.empty:
            print("Nothing to upload.")
            return None
        # print("----- UPDATING TO [{}] -----".format(self.tb_name))
        tmp_df = "./tmp_dataframe.csv"
        df.to_csv(tmp_df, index=False, header=False)
        f = open(tmp_df, 'r')
        cursor = self.up_con.cursor()
        try:
            cursor.copy_from(f, self.tb_name, sep=",", size=RDS_CONFIG["CHUNK_SIZE"])
            self.up_con.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.up_con.rollback()
            cursor.close()
            return 1
        # print("------ [{}] UPDATED ------".format(self.tb_name))
        cursor.close()

    def get_candles(self, symbol, dt_start=None, dt_end=None):
        """
        Return the selected candles records by conditions.

        :param symbol: (str) Stack abbreviation
        :param dt_start: (datetime)
        :param dt_end: (datetime)
        :return: (DataFrame)
        """
        # build statement
        stmt = select([self.current_table])
        # Add conditions
        if symbol is not None:
            stmt = stmt.where(self.current_table.columns.symbol == symbol)
        if dt_start is not None:
            stmt = stmt.where(self.current_table.columns.timestamp >= dt_start)
        if dt_end is not None:
            stmt = stmt.where(self.current_table.columns.timestamp <= dt_end)
        # Get all result
        result = self.connection.execute(stmt).fetchall()
        result = pd.DataFrame(result)
        # Check if the result contains data:
        if not result.empty:
            result.columns = ['close_price', 'high_price', 'low_price', 'open_price',
                              'status', 'timestamp', 'volume', 'symbol']
        return result

    def split_info(self, symbol, from_date, to_date):
        """
        Get the corresponding split information in the selected period.

        :param symbol:(str) Stack abbreviation
        :param from_date: (date)
        :param to_date: (date)
        :return: (DataFrame) None if not found.
        """
        try:
            split_tb = Table(RDS_CONFIG["SPLIT_TABLE"], self.metadata, autoload=True)
        except Exception as e:
            raise Exception('Please create "split_raw" table at first.')
        # build statement
        stmt = select([split_tb])
        # Add conditions
        if symbol is not None:
            stmt = stmt.where(split_tb.columns.symbol == symbol)
        if from_date is not None:
            stmt = stmt.where(split_tb.columns.date >= from_date)
        if to_date is not None:
            stmt = stmt.where(split_tb.columns.date <= to_date)
        # Get all result
        result = self.connection.execute(stmt).fetchall()
        result = pd.DataFrame(result)
        # Check if the result contains data:
        if not result.empty:
            result.columns = ['symbol', 'date', 'fromFactor', 'toFactor', 'source']
        return result

    def last_time(self, symbol):
        """
        This method returns the latest update datetime of the stack.

        :param symbol:(str) The stack abbreviation
        :return: (datetime) in UTC
        """
        # Create statement
        stmt = select([func.max(self.current_table.columns.timestamp)])
        # Filter the data by stack name
        stmt = stmt.where(self.current_table.columns.symbol == symbol)
        # Fetch the data
        result = self.connection.execute(stmt).scalar()
        return result

    def delete_stack(self, symbol):
        """
        Delete all the records of the stack in table.

        :param symbol: (str) Stack abbreviation
        :return: None. Only process operations in database.
        """
        # Count the row number pending to delete:
        count_stmt = select([func.count(self.current_table.columns.symbol)]).where(
            self.current_table.columns.symbol == symbol
        )
        to_delete = self.connection.execute(count_stmt).scalar()
        if to_delete == 0:
            return None
        else:
            delete_stmt = delete(self.current_table).where(
                self.current_table.columns.symbol == symbol)
            # Execute the statement
            deleted_num = self.connection.execute(delete_stmt).rowcount
            if deleted_num == to_delete:
                print('The records of stack [{}] have been deleted from [{}]'.format(symbol, self.tb_name))
                return None
            else:
                raise Exception("The delete numbers are not match when deleting {}".format(symbol))
