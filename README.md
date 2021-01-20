# ETL Process of Algorithmic Trading Intelligence

## Routine Steps

Step 1: 

For every stack in the list (location: ./user_info/sec_list_1000.csv), the ETL process will firstly check whether the symbol (stack name abbreviation) existed in each table, if not, it will download all the available data of this stack from 2001-01-01 to today and then load them into the corresponding table; if yes, to the next step.



Step 2:

For the stack that have already imported into database, the ETL process will firstly check it latest update time, and then compare with current time. If they are not equal, the process will then to download the "**splits**" data for the gap period, which records the splits behavior of stacks, if the returned data show there was a split/merge behavior happened, the process will delete all records of the stack, and reload them as in step 1 -- download all the data of this stack from 2001-01-01 to current day.

If there is no split, the process will then download only the data of the "gap period", and then upload it to database.



For example, if the stack AAPL has been updated by 2021-01-01, and because of some reasons (maybe database maintenance, etc.), the updating process was pausing for a week. When the ETL process check it on 2021-01-07, the process will download the "Splits" data from 2021-01-01 to 2021-01-07 and then to check if the returned records are not empty.
If yes, download only the AAPL data from 2021-01-01 to 2021-01-07, and then upload the data of only this week.

If not, that means the split/merge behavior did happen. The process will download all the AAPL data from 2001-01-01 to current day, and replace them from database to keep the data consistency.

