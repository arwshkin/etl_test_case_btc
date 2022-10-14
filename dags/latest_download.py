import asyncio
import aiohttp
import os
import time
from datetime import datetime, timedelta
from collections import ChainMap
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.log.logging_mixin import LoggingMixin
from exchangerate_utils.main import (
    DEFAULT_AIRFLOW_ARGS,
    etl_latest_data_for_ticker
)

latest_config = {
    "BTC/USD": {"source": "crypto"},
    # "EUR/USD": {"source": "ecb"},
}


start = time.time()

SCHEDULE_INTERVAL = "0 */3 * * *"
DAG_ID = "latest_download"
DEFAULT_AIRFLOW_ARGS["owner"] = "Andrey Taranov"
DEFAULT_AIRFLOW_ARGS["start_date"] = days_ago(0, hour=2)

dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_AIRFLOW_ARGS,
    schedule_interval=SCHEDULE_INTERVAL,
    # concurrency=1,
    max_active_runs=1
)

with dag:
    task_start = DummyOperator(
        task_id="start",
    )

    task_finish = DummyOperator(
        task_id="finish",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    for ticker in latest_config:
        ticker_source = latest_config[ticker]["source"]

        task_get_upload_data = PythonOperator(
            task_id=f"{ticker.replace('/', '')}_get_upload_latest_data",
            python_callable=etl_latest_data_for_ticker,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            op_kwargs={
                "ticker": ticker,
                "ticker_source": ticker_source,
            }
        )

        task_start >> task_get_upload_data >> task_finish
