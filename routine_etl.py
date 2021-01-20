"""
Routine execute script to update the newest stack data to database.
"""
from tqdm import trange
from concurrent.futures import ThreadPoolExecutor
import etl_utils.etl_main as etl
from etl_utils.etl_config import RDS_CONFIG
from stack_info import STACK_LIST


def routine_process(table_name):
    with etl.connect_table(table_name) as db_table:
        # Build process bar to estimate the routine executing time:
        total_count = len(STACK_LIST)
        # bar = trange(total_count, leave=True, desc=table_name)
        bar_count = 1
        for stack in STACK_LIST["name"]:
            # bar.set_description(stack)
            print("{} of {}:{}".format(bar_count, total_count, stack))
            etl.main_process(stack, db_table)
            # bar.update(bar_count)
            bar_count += 1
       #  bar.close()


if __name__ == '__main__':
    # executor = ThreadPoolExecutor(max_workers=2)
    #task_1 = executor.submit(routine_daily)
    #task_2 = executor.submit(routine_intraday)
    #routine_process(RDS_CONFIG['DAILY_TABLE'])
    routine_process(RDS_CONFIG['INTRADAY_TABLE'])

"""
    with etl.connect_table(RDS_CONFIG["DAILY_TABLE"]) as db_table:
        etl.main_process('SCI', db_table)

    with etl.connect_table(RDS_CONFIG["INTRADAY_TABLE"]) as db_table:
        etl.main_process('AAPL', db_table)
        
    with etl.connect_table(RDS_CONFIG["INTRADAY_TABLE"]) as db_table:
        etl.main_process('CIG', db_table)
"""
