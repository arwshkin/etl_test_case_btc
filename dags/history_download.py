import asyncio
import aiohttp
import os
import time
from datetime import datetime, timedelta
from collections import ChainMap
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.log.logging_mixin import LoggingMixin
from exchangerate_utils.main import (
    DEFAULT_AIRFLOW_ARGS,
    etl_history_data_for_ticker,
    truncate_staging,
    staging_to_target
)

history_config = {
    "BTC/USD": {"start_date": "2017-11-01", "end_date": "2022-10-10", "source": "crypto"},
    # "EUR/USD": {"start_date": "2017-01-01", "end_date": "2022-10-10", "source": "ecb"},
}

SCHEDULE_INTERVAL = "@once"
DAG_ID = "history_download"
DEFAULT_AIRFLOW_ARGS["owner"] = "Andrey Taranov"

dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_AIRFLOW_ARGS,
    schedule_interval=SCHEDULE_INTERVAL,
    # concurrency=1,
    max_active_runs=1
)

with dag:
    task_start = PythonOperator(
        task_id="truncate_staging",
        python_callable=truncate_staging
    )

    task_staging_to_target = PythonOperator(
        task_id="staging_to_target",
        python_callable=staging_to_target,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    for ticker in history_config:
        ticker_start_date = datetime.strptime(history_config[ticker]["start_date"], "%Y-%m-%d")
        ticker_end_date = datetime.strptime(history_config[ticker]["end_date"], "%Y-%m-%d")
        ticker_source = history_config[ticker]["source"]

        date_pairs = [
            [
                ticker_start_date + timedelta(days=364 * i),
                ticker_start_date + timedelta(days=364 * (i + 1))
            ] for i in
            range(ticker_end_date.year - ticker_start_date.year + 1) if
            ticker_start_date + timedelta(days=364 * i) < datetime.now()
        ]

        task_get_upload_data = PythonOperator(
            task_id=f"{ticker.replace('/', '')}_get_upload_history_data",
            python_callable=etl_history_data_for_ticker,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            op_kwargs={
                "ticker": ticker,
                "ticker_source": ticker_source,
                "date_pairs": date_pairs
            }
        )

        task_start >> task_get_upload_data >> task_staging_to_target
