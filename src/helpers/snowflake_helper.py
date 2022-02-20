import logging

from contextlib import closing
from typing import Union, Tuple, Any

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


class SnowflakeHelper():
    def __init__(self, snowflake_conn_id: str):
        self.snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    def execute_sql(self, sql, with_cursor = False) -> Union[Tuple,Any]:
        logging.info(f'The executed query is {sql}')
        
        with closing(self.snowflake_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(sql)
                res = cur.fetchall()
                if with_cursor:
                    return (res, cur)
                else:
                    return res