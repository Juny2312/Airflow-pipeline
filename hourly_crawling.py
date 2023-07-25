from airflow import DAG
from datetime import datetime, timedelta
import pendulum
import json
from pandas import json_normalize
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator



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
    dag_id = 'dynamic_agg_hdfs_system_hourly',
    start_date = datetime(2023,6,1),
    schedule_interval="@hourly",
    end_date = datetime(2023,6,18),
    default_args=default_args
  #  tags=['hour']
    )



# crawling review with product info. data
crawling_hourly = BashOperator( 
    task_id = 'preparing_data_with_crawling',
    bash_command= 'python /home/juny/code/juny_af/etl/crawl/product_n_review_to_csv.py',
    dag=base_dag
)



sleep_10 = BashOperator(
    task_id = 'sleep_10',
    bash_command = 'sleep 10',
    dag=base_dag
)


# load to hadoop file system 
to_hdfs = BashOperator(
    task_id = 'stacking_at_hdfs',
    bash_command= 'hdfs dfs -put /home/juny/code/juny_af/etl/crawl/product_n_review.csv /preparing_data/crawl/product_n_review.csv',
    dag=base_dag
)


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



load_table = gen_bash_task("load.tmp", f"hive -f {HQL_PATH}/dag-1-load-temp.hql", base_dag)

make_raw = gen_bash_task("make.raw", f"hive -f {HQL_PATH}/dag-2-make-raw.hql", base_dag)

make_base = gen_bash_task("make.base", f"hive -f {HQL_PATH}/dag-3-make-base.hql", base_dag)



# setting dag stream
crawling_hourly >> sleep_10 >>  to_hdfs >> load_table >> make_raw >> make_base

