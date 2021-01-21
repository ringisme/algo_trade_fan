"""
This is the main ETL process script,
Please ensure you have the 'etl_utils' folder in the working directory,
which have 'database_class.py' and 'finnhub_functions' inside.
Please fill up your own information in the related files in 'user_info'.
More tutorial please refer to 'README.md'
"""

import pandas as pd
from datetime import datetime, timezone, timedelta

from stack_info import STACK_NO_DATA, check_path
from etl_utils.database_class import RemoteDatabase
from etl_utils.finnhub_functions import extract_candles, extract_splits, extract_intraday
from etl_utils.etl_config import RDS_CONFIG, USER_CUSTOM


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
    # Get the gap data from API:
    if db_table.tb_name == RDS_CONFIG["DAILY_TABLE"]:
        ext_df = extract_candles(symbol, last_time, current_time)
    elif db_table.tb_name == RDS_CONFIG["INTRADAY_TABLE"]:
        ext_df = extract_intraday(symbol, last_time, current_time, db_table, upload=False)
    if ext_df.empty:
        return None
    api_vol = ext_df['volume'][ext_df.timestamp == last_time.strftime('%Y-%m-%d %H:%M:%S')]
    if api_vol.empty:
        print("No last day data found from API, upload rest gap data.")
        db_table.update_dataframe(ext_df)
        return 'missing'
    else:
        api_vol = api_vol.iloc[0]
    db_vol = db_table.get_volume(symbol, last_time)
    print("API volume:{}, DB volume:{} at {}".format(api_vol, db_vol, last_time))
    if api_vol == db_vol:
        ext_df = ext_df[ext_df.timestamp != last_time]
        if not ext_df.empty:
            db_table.update_dataframe(ext_df)
        return True
    else:
        '''
        # Upload the detect as the split record:
        split_df = pd.DataFrame({'symbol': symbol,
                                 'date': last_time.astimezone(timezone.utc).strftime('%Y-%m-%d'),
                                 'fromFactor': 0,
                                 'toFactor': 0,
                                 'source': 'detect'}, index=[0])
        # Save the detected split info into database
        with connect_table(RDS_CONFIG['SPLIT_TABLE']) as sp_table:
            sp_table.update_dataframe(split_df)
        '''

        # split happened, reload the stack:
        print(" | Split may happened during {} to {}. {} will be reloaded.".format(
            last_time.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M'),
            current_time.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M'), symbol))
        return False


def etl_reload(symbol, db_table, current_time, delete=False):
    """
    Reload all data of the stack in database.

    :param symbol: (str) Stack abbreviation
    :param db_table: (RemoteDatabase) Remote Database Object
    :param current_time: (datetime) The right end datetime
    :param delete: (boolean) To delete the stack records at first
    :return: None. Only process operations in database.
    """
    # Clear the existed records:
    if delete:
        db_table.delete_stack(symbol)
    # Reload the newest records:
    if db_table.tb_name == RDS_CONFIG['DAILY_TABLE']:
        stack_hist_df = extract_candles(symbol, current_time - timedelta(days=365 * 19), current_time)
    elif db_table.tb_name == RDS_CONFIG['INTRADAY_TABLE']:
        stack_hist_df = extract_intraday(symbol, current_time - timedelta(days=365), current_time,
                                         db_table, upload=True)
    else:
        raise Exception("The selected table is not in database. Please check the name.")
    # To check does the stack have any data:
    if not stack_hist_df.empty:
        if db_table.tb_name == RDS_CONFIG['DAILY_TABLE']:
            db_table.update_dataframe(stack_hist_df)
    else:
        if db_table.tb_name == RDS_CONFIG['INTRADAY_TABLE']:
            print("Extracted no data when reload.")
            no_data_list = STACK_NO_DATA.append(
                pd.DataFrame([[symbol]], columns=['symbol']), ignore_index=True)
            no_data_list.to_csv(check_path, index=False)
    return None


def main_process(symbol, db_table, stack_list):
    """
    Main ETL process

    :param symbol: (str) Stack abbreviation
    :param db_table: (RemoteDatabase) Remote Database Object
    :param stack_list: (list) Current existed stack list in DB
    :return: None. Only process operations in database.
    """
    if db_table.tb_name not in [RDS_CONFIG['DAILY_TABLE'], RDS_CONFIG['INTRADAY_TABLE']]:
        raise Exception("Sorry, the ETL process cannot support this table.")
    # Get the current time.
    current_time = datetime.today().astimezone(timezone.utc)
    # ----- STEP 1 -----
    # When processing the intraday data, if the stack has no intraday data, directly pass:
    if db_table.tb_name == RDS_CONFIG["INTRADAY_TABLE"] \
            and symbol in STACK_NO_DATA["symbol"].values.tolist():
        return None
    # Check if the stack already existed in database:
    if symbol not in stack_list["symbol"].values.tolist():
        print(" | {} not in table, will be reloaded".format(symbol))
        etl_reload(symbol, db_table, current_time)
        return None
    else:
        last_time = stack_list["last_time"][stack_list.symbol == symbol].iloc[0]
    # ----- STEP 2 -----
    # if the stack has already recorded in database, check the latest datetime:
    last_time = last_time.astimezone(timezone.utc)
    # Check whether the data is up to date:
    gap_duration = (current_time - last_time).total_seconds()
    gap_hours = (gap_duration / 3600)
    if gap_hours <= 24:
        if db_table.tb_name == RDS_CONFIG['DAILY_TABLE']:
            # print(" | {} is up to daily date.".format(symbol))
            return None
        elif db_table.tb_name == RDS_CONFIG['INTRADAY_TABLE']:
            if gap_hours <= USER_CUSTOM["CHECK_HOUR"]:
                return None
    # If gap period existed:
    # ----- STEP 3 -----
    # If not, extract and then upload the gap period data:
    match = etl_compare(symbol, db_table, last_time, current_time)
    if match:
        print("The gap data have been updated")
    elif match is None:
        print("No data during the gap period.")
    elif not match:
        splits_record = extract_splits(symbol, last_time, current_time)
        if splits_record.empty:  # Detect the data consistency
            print(" ------ Detect Wrong Split Data -----")
        else:
            print(" | Split happened. {} will be reloaded.".format(symbol))
            etl_reload(symbol, db_table, current_time, delete=True)

    return None
