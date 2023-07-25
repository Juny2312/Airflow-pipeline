from airflow import DAG
from datetime import datetime, timedelta
import pendulum
import json
from pandas import json_normalize
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
from airflow.decorators import task, dag # updated
from airflow.utils.dates import days_ago
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pd


# Default Args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,6,1),
    'email': ['tojuny2312@gmail.com'],
    'email_on_failure': False,
    'email_in_retry': False,
    'depends_on_past': False,
    'retries': 5 
}


base_dag = DAG(
    dag_id = 'static_agg_daily',
    start_date = datetime(2023,6,1),
    schedule_interval="@daily",
    end_date = datetime(2023,7,30), # due
    default_args=default_args,
    concurrency=3,
    max_active_runs=1,
    )


# CMD Collection
# Critoria of Household

mk_dir_static = 'hadoop fs -mkdir /etl/static'
mk_dir_cpi = 'hadoop fs -mkdir /etl/static/cpi' 
mk_dir_ppi = 'hadoop fs -mkdir /etl/static/ppi'
mk_dir_h_expediture = 'hadoop fs -mkdir /etl/static/h_expediture'
mk_dir_lt_interest = 'hadoop fs -mkdir /etl/static/lt'
mk_dir_st_interest = 'hadoop fs -mkdir /etl/static/st'

BASE_TIME = "{{data_interval_end.in_timezone('Asia/Seoul').strftime('%Y%m%d')}}"



# bash
def push_bash(name: str, cmd: str, dag, trigger="all_success"):
    bash_task = BashOperator(
        task_id=name,
        bash_command=cmd,
        trigger_rule=trigger,
        dag=base_dag
    )
    return bash_task

# Covering 6 indices At Once
# parquet 
@dag(default_args=default_args, multiple_outputs=True) 
def csvToparquet(df: pd.DataFrame): # data type : verifying json
    @task()
    def trans():
        naming = ['df_cpi','df_ppi','df_pl','df_h', 'df_lt', 'df_st']
        for nam in naming:
            df.to_parquet('{naming}.parquet')


# {PUT_PATH}
PUT_PATH = '/home/juny/code/juny_af/etl/static_data'
# {BAK_PATH}
BAK_PATH = '/home/juny/code/juny_af/etl/backup'
# {RD_PATH}
RD_PATH = '/etl/static'

# BackUp Storage Operator can be replaced by Database App.
put_cpi = 'hdfs dfs -put {PUT_PATH}/Inflation_cpi.csv /etl/static/cpi/cpi_start.csv' 
bak_cpi = 'cp /home/juny/code/juny_af/etl/temp/cpi_start.csv {BAK_PATH}/cpi_backup.csv'
echo_cpi = 'echo "cpi index was stored at {BASE_TIME}"'
#df_cpi = pd.read_csv('{RD_PATH}/cpi/cpi.csv')

#
put_ppi = 'hdfs dfs -put {PUT_PATH}/Producer_price_indices.csv /etl/static/ppi/ppi.csv'
bak_ppi = 'cp /home/juny/code/juny_af/etl/temp/ppi.csv {BAK_PATH}/ppi_backup.csv'
echo_ppi = 'echo "ppi index was stored at {BASE_TIME}"'
#df_ppi = pd.read_csv('{RD_PATH}/ppi/ppi.csv')

put_pl = 'hdfs dfs -put {PUT_PATH}/Price_level_indices.csv /etl/static/pl/price_level.csv' 
bak_pl = 'cp /home/juny/code/juny_af/etl/temp/price_level.csv {BAK_PATH}/price_level_backup.csv'
echo_pl = 'echo "Index of Price Level was stored at {BASE_TIME}"'
#df_pl = pd.read_csv('{RD_PATH}/pl/price_level.csv')

put_h = 'hdfs dfs -put {PUT_PATH}/Household_spending.csv /etl/static/h_expenditure/h_spending.csv'
bak_h = 'cp /home/juny/code/juny_af/etl/temp/h-spending.csv {BAK_PATH}/h_spending_backup.csv'
echo_h = 'echo "Index of Household expediture was stored at {BASE_TIME}"'
#df_h = pd.read_csv('{RD_PATH}/h_expenditure/h_expenditure.csv')

put_lt = 'hdfs dfs -put {PUT_PATH}/Long_term_interest.csv /etl/static/lt/lt_interest.csv'
bak_lt = 'cp /home/juny/code/juny_af/etl/temp/lt_interest.csv {BAK_PATH}/lt_interest_backup.csv'
echo_lt = '"echo long term interest was stored at {BASE_TIME}"'
#df_lt = pd.read_csv('{RD_PATH}/lt/lt_interest.csv')

put_st = 'hdfs dfs -put {PUT_PATH}/Short_term_interest.csv /etl/static/st/st_interest.csv' 
bak_st = 'cp /home/juny/code/juny_af/etl/temp/st_interest.csv {BAK_PATH}/st_interest_backup.csv'
echo_st = '"echo short term interest was stored at {BASE_TIME}"'
#df_st =  pd.read_csv('{RD_PATH}/st/st_interest.csv')


# Done log
DONE_log = BashOperator(
    task_id = 'UPDATED_DONE',
    bash_comman= done_cpi,
    dag=base_dag
)

#------------------------------------------------------------

# cpi
cpi_start = BashOperator(
    task_id='cpi_start',
    bash_command= put_cpi,
    dag=base_dag
)
# back-up file (split or delete it when you want)
cpi_make_backup = BashOperator(
    task_id='cpi_make_backup',
    bash_command= bak_cpi,
    dag=base_dag
) 

cpi_echo = BashOperator(
    task_id='echo_cpi',
    bash_command= echo_cpi,
    dag=base_dag,
)

#---------------------------------------------------------

#ppi
ppi_start = BashOperator(
    task_id='ppi_start',
    bash_command= put_ppi,
    dag=base_dag
)
# back-up file (split or delete it when you want)
ppi_make_backup = BashOperator(
    task_id='ppi_make_backup',
    bash_command= bak_ppi ,
    dag=base_dag
)

ppi_echo = BashOperator(
    task_id='echo_ppi',
    bash_command= echo_cpi,
    dag=base_dag,
)

#--------------------------------------------------------

#price level
prlev_start = BashOperator(
    task_id='prlev_start',
    bash_command= put_pl,
    dag=base_dag
)

# back-up file (split or delete it when you want)
prlev_make_backup = BashOperator(
    task_id='prlev_make_backup',
    bash_command= bak_pl,
    dag=base_dag
)

pl_echo = BashOperator(
    task_id='echo_pl',
    bash_command= echo_pl,
    dag=base_dag,
)

#--------------------------------------------------------

#h spending 

h_spending = BashOperator(
    task_id='h_spending_start',
    bash_command= put_h,
    dag=base_dag
)

# back-up file (split or delete it when you want)
h_spending_make_backup = BashOperator(
    task_id='h_spending_make_backup',
    bash_command= bak_h,
    dag=base_dag
)

h_echo = BashOperator(
    task_id='echo_h',
    bash_command= echo_h,
    dag=base_dag,
)

#--------------------------------------------------------

#lt interest
lt_interest_start = BashOperator(
    task_id='lt_interest_start',
    bash_command= put_lt,
    dag=base_dag
)


# back-up file (split or delete it when you want)
lt_interest_make_backup = BashOperator(
    task_id='lt_interest_backup',
    bash_command= bak_lt,
    dag=base_dag
)

lt_echo = BashOperator(
    task_id='echo_lt',
    bash_command= echo_lt,
    dag=base_dag,
)

#---------------------------------------------------------

#st interest

st_interest_start = BashOperator(
    task_id='st_interest_start',
    bash_command= put_st,
    dag=base_dag
)


# back-up file (split or delete it when you want)
st_interest_make_backup = BashOperator(
    task_id='st_interest_make_backup',
    bash_command= bak_st,
    dag=base_dag
)

st_echo = BashOperator(
    task_id='echo_st',
    bash_command= echo_st,
    dag=base_dag,
)


#---------------------------------------------------------

df_cpi = pd.read_csv('{RD_PATH}/cpi/cpi.csv')
df_ppi = pd.read_csv('{RD_PATH}/ppi/ppi.csv')
df_pl = pd.read_csv('{RD_PATH}/pl/price_level.csv')
df_h = pd.read_csv('{RD_PATH}/h_expenditure/h_expenditure.csv')
df_lt = pd.read_csv('{RD_PATH}/lt/lt_interest.csv')
df_st =  pd.read_csv('{RD_PATH}/st/st_interest.csv')


dfli = [ df_cpi, df_ppi, df_pl, df_h, df_lt, df_st ]



@dag(default_args=default_args, multiple_outputs=True)
def execdf():
    @task
    def exec():
        for li in dfli:
            exec_f_transform = csvToparquet(li)

#  
#start >> load_temp >> raw >> base(reduce) >> Done
#start >> bak

# Verify-ing Streams and ThroubleShoot-ing 








