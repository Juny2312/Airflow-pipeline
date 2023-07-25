from airflow import DAG
from datetime import datetime, timedelta
import pendulum
import json
from pandas import json_normalize
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
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
    dag_id = 'static_agg_hdfs_system_daily',
    start_date = datetime(2023,6,1),
    schedule_interval="@daily",
    end_date = datetime(2023,7,30), # regualtion requests
    default_args=default_args,
    #dagrun_timeout=timedelta(minutes=1),
    #schedule_interval='* * * * *'

  #  tags=['hour']
    )



# crawling review with product info. data
#crawling_hourly = BashOperator( 
#    task_id = 'preparing_data_with_crawling',
#    bash_command= 'python /home/juny/code/juny_af/etl/crawl/product_n_review.py',
#    dag=base_dag
#)

# load to hadoop file system 
#to_hdfs = BashOperator(
#    task_id = 'stacking_at_hdfs',
#    bash_command= 'hdfs dfs -put /home/juny/code/juny_af/etl/crawl/product_n_review.csv /basement/hive/crawling/product_n_review.csv',
#    dag=base_dag
#)


### Define Bash task
#check = BashOperator(
#    task_id='check',
#    bash_command="""
#    """,
#    retires = 3,
#    retry_delay = timedelta(seconds = 1),
#    dag=base_dag
#)


### get data : patterns ###


# you can make first 'check' file for existance. 


#to_hdfs = BashOperator(
#    task_id = 'stacking_at_hdfs',
#    bash_command= 'hdfs dfs -put /home/juny/code/juny_af/etl/crawl/product_n_review.csv /basement/hive/crawling/product_n_review.csv',
#    dag=base_dag
#)

# cpi
cpi_start = BashOperator(
    task_id='cpi_start',
    bash_command="""
    hdfs dfs -put /home/juny/code/juny_af/etl/static_data/Inflation_cpi.csv /etl/static_data/temp/cpi/cpi_start.csv
    """,
    dag=base_dag
)


# back-up file (split or delete it when you want)
cpi_make_raw_backup = BashOperator(
    task_id='cpi_make_raw_backup',
    bash_command="""
    cp /home/juny/code/juny_af/etl/temp/cpi_start.csv /home/juny/code/juny_af/etl/raw_to_archive/cpi_backup.csv    
    """,
    dag=base_dag
) 

# you can make sum file if you get through editing files with bash 
# make send delete 



### aggregation part is below ###
# start (setting pos) > make_raw (for backup) > aggregation with hql 3 steps file
# (When you want to join, It needs more dag with function with hql) >
# > 
################################
################################################################

# DONE log file after making qarquet in the haddop warehouse.

hdfs_DONE = BashOperator(
    task_id = 'hdfs_UPDATED_DONE',
    bash_command ="""
    touch /home/juny/code/juny_af/etl/parquet/log/DONE
    """,
    dag=base_dag
)

#################################################################

#ppi

ppi_start = BashOperator(
    task_id='ppi_start',
    bash_command="""
    hdfs dfs -put /home/juny/code/juny_af/etl/static_data/Producer_price_indices.csv /etl/static_data/temp/ppi/ppi.csv
    """,
    dag=base_dag
)


# back-up file (split or delete it when you want)
ppi_make_raw_backup = BashOperator(
    task_id='ppi_make_raw',
    bash_command="""
    cp /home/juny/code/juny_af/etl/temp/ppi.csv /home/juny/code/juny_af/etl/raw_to_archive/ppi_backup.csv
    """,
    dag=base_dag
)




#price level


prlev_start = BashOperator(
    task_id='prlev_start',
    bash_command="""
    hdfs dfs -put /home/juny/code/juny_af/etl/static_data/Price_level_indices.csv /etl/static_data/temp/prelev/price_level.csv
    """,
    dag=base_dag
)


# back-up file (split or delete it when you want)
prlev_make_raw_backup = BashOperator(
    task_id='prlev_make_raw',
    bash_command="""
    cp /home/juny/code/juny_af/etl/temp/price_level.csv /home/juny/code/juny_af/etl/raw_to_archive/price_level_backup.csv
    """,
    dag=base_dag
)



#h spending 

h_spending = BashOperator(
    task_id='h_spending_start',
    bash_command="""
    hdfs dfs -put /home/juny/code/juny_af/etl/static_data/Household_spending.csv /etl/static_data/temp/h_spending/h_spending.csv
    """,
    dag=base_dag
)


# back-up file (split or delete it when you want)
h_spending_make_raw_backup = BashOperator(
    task_id='h_spending_make_raw',
    bash_command="""
    cp /home/juny/code/juny_af/etl/temp/h-spending.csv /home/juny/code/juny_af/etl/raw_to_archive/h_spending_backup.csv
    """,
    dag=base_dag
)



#lt interest
lt_interest_start = BashOperator(
    task_id='lt_interest_start',
    bash_command="""
    hdfs dfs -put /home/juny/code/juny_af/etl/static_data/Long_term_interest.csv /etl/static_data/temp/lt_interest/lt_interest.csv
    """,
    dag=base_dag
)


# back-up file (split or delete it when you want)
lt_interest_make_raw_backup = BashOperator(
    task_id='lt_interest_make_raw',
    bash_command="""
    cp /home/juny/code/juny_af/etl/temp/lt_interest.csv /home/juny/code/juny_af/etl/raw_to_archive/lt_interest_backup.csv
    """,
    dag=base_dag
)




#st interest

st_interest_start = BashOperator(
    task_id='st_interest_start',
    bash_command="""
    hdfs dfs -put /home/juny/code/juny_af/etl/static_data/Short_term_interest.csv /etl/static_data/temp/st_interest/st_interest.csv
    """,
    dag=base_dag
)


# back-up file (split or delete it when you want)
st_interest_make_raw_backup = BashOperator(
    task_id='st_interest_make_raw',
    bash_command="""
    cp /home/juny/code/juny_af/etl/temp/st_interest.csv /home/juny/code/juny_af/etl/raw_to_archive/st_interest_backup.csv
    """,
    dag=base_dag
)




#### aggregation ####
def gen_bash_task(name: str, cmd: str, dag, trigger="all_success"):
    """airflow bash task 생성
        - trigger-rules : https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#trigger-rules
    """

    bash_task = BashOperator(
        task_id=name,
        bash_command=cmd,
        trigger_rule=trigger,
        dag=base_dag
    )
    return bash_task



HQL_PATH = '/home/juny/code/juny_af/etl/stack_hql'


# 1
pricelev_load_temp = gen_bash_task("pricelev_load.tmp", f"hive -f {HQL_PATH}/pricelev-1-load-temp.hql", base_dag)

pricelev_make_raw = gen_bash_task("pricelev_make.raw", f"hive -f {HQL_PATH}/pricelev-2-make-raw.hql", base_dag)

pricelev_make_base = gen_bash_task("pricelev_make.base", f"hive -f {HQL_PATH}/pricelev-3-make-base.hql", base_dag)

# 2
ppi_load_temp = gen_bash_task("ppi_load.tmp", f"hive -f {HQL_PATH}/ppi-1-load-temp.hql", base_dag)

ppi_make_raw = gen_bash_task("ppi_make.raw", f"hive -f {HQL_PATH}/ppi-2-make-raw.hql", base_dag)

ppi_make_base = gen_bash_task("ppi_make.base", f"hive -f {HQL_PATH}/ppi-3-make-base.hql", base_dag)

# 3
cpi_load_temp = gen_bash_task("cpi_load.tmp", f"hive -f {HQL_PATH}/cpi-1-load-temp.hql", base_dag)

cpi_make_raw = gen_bash_task("cpi_make.raw", f"hive -f {HQL_PATH}/cpi-2-make-raw.hql", base_dag)

cpi_make_base = gen_bash_task("cpi_make.base", f"hive -f {HQL_PATH}/cpi-3-make-base.hql", base_dag)


# 4
h_spending_load_temp = gen_bash_task("h_spending_load.tmp", f"hive -f {HQL_PATH}/h-spending-1-load-temp.hql", base_dag)

h_spending_make_raw = gen_bash_task("h_spending_make.raw", f"hive -f {HQL_PATH}/h-spending-2-make-raw.hql", base_dag)

h_spending_make_base = gen_bash_task("h_spending_make.base", f"hive -f {HQL_PATH}/h-spending-3-make-base.hql", base_dag)

# 5
lt_interest_load_temp = gen_bash_task("lt_interest_load.tmp", f"hive -f {HQL_PATH}/lt-interest-1-load-temp.hql", base_dag)

lt_interest_make_raw = gen_bash_task("lt_interest_make.raw", f"hive -f {HQL_PATH}/lt-interest-2-make-raw.hql", base_dag)

lt_interest_make_base = gen_bash_task("lt_interest_make.base", f"hive -f {HQL_PATH}/lt-interest-3-make-base.hql", base_dag)


# 6
st_interest_load_temp = gen_bash_task("st_interest_load.tmp", f"hive -f {HQL_PATH}/st-interest-1-load-temp.hql", base_dag)

st_interest_make_raw = gen_bash_task("st_interest_make.raw", f"hive -f {HQL_PATH}/st-interest-2-make-raw.hql", base_dag)

st_interest_make_base = gen_bash_task("st_interest_make.base", f"hive -f {HQL_PATH}/st-interest-3-make-base.hql", base_dag)






# setting dag stream
# 1
#pricelev_load_temp >> pricelev_make_raw >> pricelev_make_base
# 2
#ppi_load_temp >> ppi_make_raw >> ppi_make_base
# 3
#cpi_load_temp >> cpi_make_raw >> cpi_make_base
# 4
#h_spending_load_temp >> h_spending_make_raw >> h_spending_make_base
# 5
#lt_interest_load_temp >> lt_interest_make_raw >> lt_interest_make_base
# 6
#st_interest_load_temp >> st_interest_make_raw >> st_interest_make_base

# 1  
cpi_start >> cpi_load_temp >> cpi_make_raw >> cpi_make_base >> hdfs_DONE
cpi_start >> cpi_make_raw_backup
# 2
ppi_start >> ppi_load_temp >> ppi_make_raw >> ppi_make_base >> hdfs_DONE
ppi_start >> ppi_make_raw_backup
# 3
prlev_start >> pricelev_load_temp >> pricelev_make_raw >> pricelev_make_base >> hdfs_DONE
prlev_start >> prlev_make_raw_backup

# 4
h_spending >> h_spending_load_temp >> h_spending_make_raw >> h_spending_make_base >> hdfs_DONE
h_spending >> h_spending_make_raw_backup

# 5
lt_interest_start >> lt_interest_load_temp >> lt_interest_make_raw >> lt_interest_make_base >> hdfs_DONE
lt_interest_start >> lt_interest_make_raw_backup


# 6
st_interest_start >> st_interest_load_temp >> st_interest_make_raw >> st_interest_make_base >> hdfs_DONE
st_interest_start >> st_interest_make_raw_backup







