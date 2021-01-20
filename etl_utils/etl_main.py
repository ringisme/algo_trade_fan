"""
This is the main ETL process script,
Please ensure you have the 'etl_utils' folder in the working directory,
which have 'database_class.py' and 'finnhub_functions' inside.
Please fill up your own information in the related files in 'user_info'.
More tutorial please refer to 'README.md'
"""

import pandas as pd
from datetime import datetime, timezone, timedelta

from etl_utils.database_class import RemoteDatabase
from etl_utils.finnhub_functions import extract_candles, extract_splits, extract_intraday
from etl_utils.etl_config import RDS_CONFIG


def connect_table(table_name):
    """
    Connect the table on RDS postgreSQL database.

    :param table_name: (str) Table name
    :return: (RemoteDatabase) returns None if no table found.
    """
    user_name, password, host = RDS_CONFIG["USERNAME"], RDS_CONFIG["PASSWORD"], RDS_CONFIG["HOST"]
    try:
        db_table = RemoteDatabase(tb_name=table_name, user_name=user_name,
                                  password=password, endpoint=host)
        return db_table
    except Exception as e:
        raise Exception("Cannot build the connection because of {}".format(e.__class__))


def etl_check_split(symbol, start_date, end_date):
    """
    Check the split behavior for specific period.

    :param symbol: (str) Stack abbreviation
    :param start_date: (datetime)
    :param end_date: (datetime)
    :return: (DataFrame) includes split information. Empty DataFrame if none.
    """
    # Firstly check the Finnhub split info:
    res_api = extract_splits(symbol, start_date, end_date)
    with connect_table(RDS_CONFIG['SPLIT_TABLE']) as db_table:
        res_db = db_table.split_info(symbol, start_date.astimezone(timezone.utc),
                                     end_date.astimezone(timezone.utc))
        # Upload the record to database if not exist:
        if not res_api.empty and res_db.empty:
            db_table.update_dataframe(res_api)
            return res_api
        if not res_api.empty and not res_db.empty:
            return res_db
        # Check if the database has any detected split record:
        else:
            return res_db


def etl_compare(symbol, db_table, last_time, current_time):
    if db_table.tb_name == RDS_CONFIG["DAILY_TABLE"]:
        ext_df = extract_candles(symbol, last_time, current_time)
    elif db_table.tb_name == RDS_CONFIG["INTRADAY_TABLE"]:
        ext_df = extract_intraday(symbol, last_time, current_time)
    # Start to compare:
    if ext_df.empty:
        return True
    # Assign the first raw as the check raw:
    api_df = ext_df.iloc[[0]]
    gap_df = ext_df.iloc[1:]
    # Check with the database record:
    com_df = db_table.get_candles(symbol,
                                  last_time.astimezone(timezone.utc),
                                  last_time.astimezone(timezone.utc))
    # If the records doesn't match, record it into split:
    if api_df['volume'][0] != com_df['volume'][0]:
        split_df = pd.DataFrame({'symbol': symbol,
                                 'date': last_time.astimezone(timezone.utc).strftime('%Y-%m-%d'),
                                 'fromFactor': 0,
                                 'toFactor': 0,
                                 'source': 'detect'}, index=[0])
        with connect_table(RDS_CONFIG['SPLIT_TABLE']) as sp_table:
            sp_table.update_dataframe(split_df)
        # split happened, reload the stack:
        print(" | Split happened during {} to {}. {} will be reloaded.".format(
            last_time.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M'),
            current_time.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M'), symbol))
        return False
    else:
        # print("{} detected same values".format(symbol))
        if not gap_df.empty:
            db_table.update_dataframe(gap_df)
        return True


def etl_reload(symbol, db_table, current_time):
    """
    Reload all data of the stack in database.

    :param symbol: (str) Stack abbreviation
    :param db_table: (RemoteDatabase) Remote Database Object
    :param current_time: (datetime) The right end datetime
    :return: None. Only process operations in database.
    """
    # Clear the existed records:
    db_table.delete_stack(symbol)
    # Reload the newest records:
    if db_table.tb_name == RDS_CONFIG['DAILY_TABLE']:
        stack_hist_df = extract_candles(symbol, current_time - timedelta(days=365 * 19), current_time)
    elif db_table.tb_name == RDS_CONFIG['INTRADAY_TABLE']:
        stack_hist_df = extract_intraday(symbol, current_time - timedelta(days=365), current_time)
    else:
        raise Exception("The selected table is not in database. Please check the name.")
    db_table.update_dataframe(stack_hist_df)
    return None


def main_process(symbol, db_table):
    """
    Main ETL process

    :param symbol: (str) Stack abbreviation
    :param db_table: (RemoteDatabase) Remote Database Object
    :return: None. Only process operations in database.
    """
    if db_table.tb_name not in [RDS_CONFIG['DAILY_TABLE'], RDS_CONFIG['INTRADAY_TABLE']]:
        raise Exception("Sorry, the ETL process cannot support this table.")
    current_time = datetime.today().astimezone(timezone.utc)
    # ----- STEP 1 -----
    if symbol not in db_table.stack_list:
        print(" | {} not in table, will be reloaded".format(symbol))
        etl_reload(symbol, db_table, current_time)
        return None
    # ----- STEP 2 -----
    # if the stack has already recorded in database, check the latest datetime:
    last_time = db_table.last_time(symbol)
    last_time = last_time.astimezone(timezone.utc)
    # Check whether the data is up to date:
    if last_time.strftime('%Y-%m-%d') == current_time.strftime('%Y-%m-%d'):
        if db_table.tb_name == RDS_CONFIG['DAILY_TABLE']:
            print(" | {} is up to daily date.".format(symbol))
            return None
        elif db_table.tb_name == RDS_CONFIG['INTRADAY_TABLE']:
            gap_duration = (last_time-current_time).total_seconds()
            if gap_duration/3600 <= 2:
                print(" | {} is up to intraday date.".format(symbol))
                return None
            else:
                ext_df = extract_intraday(symbol, last_time, current_time)
                if len(ext_df) < 2:
                    return None
                else:
                    db_table.update_dataframe(ext_df.iloc[1:])
                    return True
    # ----- STEP 3 -----
    # If not, extract and then upload the gap period data:
    match = etl_compare(symbol, db_table, last_time, current_time)
    if match:
        return None
    else:
        splits_record = etl_check_split(symbol, last_time, current_time)
        if splits_record.empty:  # Detect the data consistency
            print(" ------ Detect Wrong Split Data -----")
            return None
        else:
            print(" | Split happened. {} will be reloaded.".format(symbol))
            etl_reload(symbol, db_table, current_time)
            return None
