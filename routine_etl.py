"""
Routine execute script to update the newest stack data to database.
"""
from tqdm import tqdm
from twilio.rest import Client
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import etl_utils.etl_main as etl
from etl_utils.etl_config import RDS_CONFIG, TWILIO_CONFIG
from stack_info import STACK_LIST


def etl_main_process(table_name):
    """
    The main process to execute the ETL, download data from Finnhub and upload to RDS database.

    :param table_name: (str) Database default table name
    :return: None
    """

    with etl.connect_table(table_name) as db_table:
        stack_list = db_table.stack_list()
        # Build process bar to estimate the routine executing time:
        t_stack_list = tqdm(STACK_LIST["name"])
        tb_name = db_table.tb_name
        for stack in t_stack_list:
            t_stack_list.set_description("{} | {}".format(tb_name, stack))
            etl.main_process(stack, db_table, stack_list)


def routine_process(phone_alert=False, multi_process=False):
    """
    The routine process to update the newest stack data to database automatically.

    :param multi_process:
    :param table_name: (str) Database default table name
    :param alert: (boolean) set True to send Error message to assigned phone number
    :return: None
    """
    start_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    # Set the Phone alert:
    account_sid = TWILIO_CONFIG["ACCOUNT_SID"]
    auth_token = TWILIO_CONFIG["AUTH_TOKEN"]
    client = Client(account_sid, auth_token)

    try:
        if multi_process:
            # Set the multiply threads:
            executor = ThreadPoolExecutor(max_workers=2)
            task_1 = executor.submit(etl_main_process, RDS_CONFIG["DAILY_TABLE"])
            task_2 = executor.submit(etl_main_process, RDS_CONFIG["INTRADAY_TABLE"])
            all_task = [task_1, task_2]
            for task in as_completed(all_task):
                print(task.result())
        else:
            etl_main_process(RDS_CONFIG["DAILY_TABLE"])
            etl_main_process(RDS_CONFIG["INTRADAY_TABLE"])
        end_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        print("Routine task starts from {}, successful finished at {}.".format(start_time, end_time))
    except Exception as e:
        end_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        alert_info = "Because of {}, the {} was interrupted on {}.".format(e.__class__,
                                                                           RDS_CONFIG["INTRADAY_TABLE"],
                                                                           end_time)
        if phone_alert:
            message = client.messages.create(from_=TWILIO_CONFIG["TWILIO_PHONE"],
                                             to=TWILIO_CONFIG["USER_PHONE"],
                                             body=alert_info)
            print(message.sid)
        raise Exception(alert_info)


if __name__ == '__main__':
    routine_process()
