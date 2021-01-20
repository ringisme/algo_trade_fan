import pandas as pd
from os.path import join, dirname

csv_path = join(dirname(__file__), 'sec_list_1000.csv')
STACK_LIST = pd.read_csv(csv_path)
