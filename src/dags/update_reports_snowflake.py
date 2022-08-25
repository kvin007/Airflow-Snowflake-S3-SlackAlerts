"""
The dag inserts information regarding the number of members joined to a specific group in each day.
It shall get data from previous date and insert it into the table
"""

import logging
import pandas as pd
import pytz

from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Any

from src.helpers.snowflake_helper import SnowflakeHelper
from src.helpers.dag_helper import get_dag_default_args
from src.helpers.aws_s3_helper import AWSS3Helper

from airflow import DAG
from airflow.decorators import task, branch_task
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.edgemodifier import Label


SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
AWS_S3_CONN_ID = 'aws_s3_conn_id'
DAYS_BACK_IN_TIME = 10 * 365
DEFAULT_BUCKET_NAME = 'my-airflow-bucket-kvinp007'
snowflake_helper = SnowflakeHelper(SNOWFLAKE_CONN_ID)

def get_previous_day_for_processing() -> str:
    time_zone = pytz.timezone('UTC') # Just as a test
    previous_day = datetime.strftime(datetime.now(time_zone) - timedelta(days=DAYS_BACK_IN_TIME),'%Y-%m-%d')
    return previous_day


with DAG(
    dag_id='update_reports_snowflake',
    default_args=get_dag_default_args(),
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__
) as dag:

    @task(multiple_outputs=True)
    def verify_destination_created() -> Dict[str, bool]:
        verify_query = """
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE  table_schema = 'MEETUP'
        AND    table_name   = 'DAILY_JOINED'
        """
        
        result = snowflake_helper.execute_sql(verify_query, with_cursor=False)[0][0]
        logging.info(f'The result is {result}')

        return {
            "is_created": result == 1
        }

    @branch_task(provide_context=True)
    def check_need_table(**kwargs) -> str:
        ti = kwargs['ti']
        xcom_value = ti.xcom_pull(task_ids='verify_destination_created')
        return 'insert_members_joined' if xcom_value['is_created'] else 'create_daily_joined_table'

    @task()
    def create_daily_joined_table() -> None:
        create_statement = """
        CREATE OR REPLACE TABLE YOUR_DATABASE.MEETUP.daily_joined(
            date_group     text,
            joined_date    date,
            group_id       bigint,
            group_name     text,
            members_joined int
        );
        """
        
        snowflake_helper.execute_sql(create_statement, with_cursor=False)

    @task(trigger_rule='none_failed')
    def insert_members_joined() -> None:
        previous_day = get_previous_day_for_processing()
        
        merge_statement = """
        MERGE INTO YOUR_DATABASE.MEETUP.daily_joined
        USING
        (SELECT CONCAT(TO_VARCHAR(TO_DATE(aux.joined_date)),'_',TO_VARCHAR(aux.group_id)) as date_group, aux.*
            FROM
            (
                SELECT TO_DATE(m.joined) as joined_date, m.group_id, g.group_name, count(*) as members_joined
                FROM YOUR_DATABASE.MEETUP.members m
                INNER JOIN YOUR_DATABASE.MEETUP.groups g
                ON m.group_id = g.group_id
                WHERE TO_DATE(m.joined) = TO_DATE(""" + "'" + previous_day + "'" + """)
                GROUP BY TO_DATE(m.joined), m.group_id, g.group_name
            ) as aux
        ) as src
        ON YOUR_DATABASE.MEETUP.daily_joined.date_group = src.date_group
        WHEN NOT MATCHED THEN
        INSERT (date_group,joined_date,group_id,group_name, members_joined) VALUES (src.date_group,src.joined_date,src.group_id,src.group_name,src.members_joined)
        """

        snowflake_helper.execute_sql(merge_statement, with_cursor=False)

    @task()
    def upload_to_aws_s3_bucket() -> None:
        # I just consider 100 since this is for testing purposes
        current_day_members = """
            SELECT * FROM YOUR_DATABASE.MEETUP.daily_joined LIMIT 100;
        """
        previous_day = get_previous_day_for_processing()
        result, cursor = snowflake_helper.execute_sql(current_day_members, with_cursor=True)
        headers = list(map(lambda t: t[0], cursor.description))
        df = pd.DataFrame(result)
        df.columns = headers
        
        local_path = 'data.csv' #Should get path from Worker (or Persistent Volume Claim folder)
        df.to_csv(local_path, header=True, mode='w', sep=',')
        destination_name = f'{previous_day}.csv'

        logging.info(f'The {local_path} file will be copied to {destination_name} in {DEFAULT_BUCKET_NAME}')

        aws_helper = AWSS3Helper(AWS_S3_CONN_ID)
        aws_helper.upload_file(
            local_file_path=local_path,
            destination_name=destination_name,
            bucket_name=DEFAULT_BUCKET_NAME,
            replace=True,
        )

    
    verify_destination = verify_destination_created()
    check_table = check_need_table()
    create_destination = create_daily_joined_table()
    insert_data = insert_members_joined()
    upload_s3 = upload_to_aws_s3_bucket()

    verify_destination >> check_table
    check_table >> Label("Destination does not exist") >> create_destination >> insert_data >> upload_s3
    check_table >> Label("Insert data") >> insert_data >> upload_s3