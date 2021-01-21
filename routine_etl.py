"""
Routine execute script to update the newest stack data to database.
"""
from tqdm import tqdm
from twilio.rest import Client
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import etl_utils.etl_main as etl
from etl_utils.etl_config import RDS_CONFIG, TWILIO_CONFIG
from stack_info import STACK_LIST


def routine_process(table_name):
    with etl.connect_table(table_name) as db_table:
        # Build process bar to estimate the routine executing time:
        t_stack_list = tqdm(STACK_LIST["name"])
        for stack in t_stack_list:
            t_stack_list.set_description(stack)
            etl.main_process(stack, db_table)


if __name__ == '__main__':
    # Set the Phone alert:
    account_sid = TWILIO_CONFIG["ACCOUNT_SID"]
    auth_token = TWILIO_CONFIG["AUTH_TOKEN"]
    client = Client(account_sid, auth_token)

    # Start the main process:
    try:
        process_start_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        routine_process(RDS_CONFIG["INTRADAY_TABLE"])
    except Exception as e:
        process_end_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        alert_info = "Because of {}, the {} was interrupted on {}.".format(e.__class__,
                                                                           RDS_CONFIG["INTRADAY_TABLE"],
                                                                           process_end_time)
        message = client.messages.create(from_=TWILIO_CONFIG["TWILIO_PHONE"],
                                         to=TWILIO_CONFIG["USER_PHONE"],
                                         body=alert_info)
        print(message.sid)
        raise Exception(alert_info)





"""
# executor = ThreadPoolExecutor(max_workers=2)
    # task_1 = executor.submit(routine_daily)
    # task_2 = executor.submit(routine_intraday)
    # routine_process(RDS_CONFIG['DAILY_TABLE'])

    with etl.connect_table(RDS_CONFIG["DAILY_TABLE"]) as db_table:
        etl.main_process('SCI', db_table)

    with etl.connect_table(RDS_CONFIG["INTRADAY_TABLE"]) as db_table:
        etl.main_process('AAPL', db_table)
        
    with etl.connect_table(RDS_CONFIG["INTRADAY_TABLE"]) as db_table:
        etl.main_process('CIG', db_table)
    
    etl.main_process('CCXX', db_table)
"""
