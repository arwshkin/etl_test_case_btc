from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import asyncio
import aiohttp
from collections import ChainMap
import pandas as pd
from sqlalchemy import create_engine
import requests
from typing import List
import os

history_url = 'https://api.exchangerate.host/timeseries?base={base}&symbols={symbols}&source={source}&start_date={start_date}&end_date={end_date}'
latest_url = 'https://api.exchangerate.host/latest?base={base}&symbols={symbols}&source={source}'

DATA_DB_CONNECTION_STRING = os.getenv("DATA_DB_CONNECTION_STRING")

DEFAULT_AIRFLOW_ARGS = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "pool": "default_pool",
    "max_active_runs": 1,
}


def get_history_tasks(session: aiohttp.ClientSession,
                      _ticker: str,
                      _ticker_source: str,
                      _date_pairs: List[List[datetime]]) -> List[asyncio.Task]:
    """
    Creates a list of tasks for asynchronous unloading of historical data on the ticker of
        interest for the period of interest.

    :param session: aiohttp.ClientSession object to make asynchronous API requests
    :param _ticker: Ticker name
    :param _ticker_source: Ticker source. Could be 'ecb' or 'crypto'
    :param _date_pairs: List of pairs of dates (each one of one year) between start_date and end_date

    :return: Task list for asynchronous requests
    """
    tasks = []
    for date_pair in _date_pairs:
        tasks.append(asyncio.create_task(session.get(history_url.format(base=_ticker.split('/')[0],
                                                                        symbols=_ticker.split('/')[1],
                                                                        source=_ticker_source,
                                                                        start_date=date_pair[0].strftime('%Y-%m-%d'),
                                                                        end_date=date_pair[1].strftime('%Y-%m-%d')),
                                                     ssl=False)))
    return tasks


def etl_history_data_for_ticker(ticker: str,
                                ticker_source: str,
                                date_pairs: List[List[datetime]]) -> None:
    """
    Downloads historical data in asynchronous mode according to the list of tasks
    The result is put into postgres.

    :param ticker: Ticker name.
    :param ticker_source: Ticker source. Could be 'ecb' or 'crypto'
    :param date_pairs: List of pairs of dates (each one of one year) between start_date and current time
    :return: None
    """
    results = []

    async def get_currencies(_ticker, _ticker_source, _date_pairs):
        async with aiohttp.ClientSession() as session:
            tasks = get_history_tasks(session, _ticker, _ticker_source, _date_pairs)
            responses = await asyncio.gather(*tasks)
            for response in responses:
                results.append(await response.json())

    asyncio.run(get_currencies(ticker, ticker_source, date_pairs))

    results = [date['rates'] for date in results]
    results = dict(ChainMap(*results))

    results = [
        {
            "ticker_name": ticker,
            "ticker_timestamp": datetime.strptime(date, '%Y-%m-%d'),
            "ticker_value": results[date][ticker.split('/')[1]]
        }
        for date in results if ticker.split('/')[1] in results[date]
    ]

    df = pd.DataFrame(results)

    df.to_sql(
        "currencies_staging",
        con=create_engine(DATA_DB_CONNECTION_STRING),
        if_exists='append',
        index=False,
        chunksize=365
    )


def etl_latest_data_for_ticker(ticker: str,
                               ticker_source: str) -> None:
    """
    Downloads latest ticker value on the ticker.
    The result is put into postgres.

    :param ticker: Ticker name.
    :param ticker_source: Ticker source. Could be 'ecb' or 'crypto'
    :return: None
    """
    req = requests.get(latest_url.format(
        base=ticker.split('/')[0],
        symbols=ticker.split('/')[1],
        source=ticker_source,
    ))

    engine = create_engine(DATA_DB_CONNECTION_STRING)

    sql = "INSERT INTO currencies VALUES ('{ticker_name}', '{ticker_timestamp}', {ticker_value})"

    with engine.begin() as conn:
        conn.execute(sql.format(
            ticker_name=ticker,
            ticker_timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            ticker_value=req.json()["rates"][ticker.split("/")[1]]
        ))


def truncate_staging() -> None:
    """
    Clears the staging table before uploading historical data

    :return: None
    """
    engine = create_engine(DATA_DB_CONNECTION_STRING)

    sql = "TRUNCATE currencies_staging"

    with engine.begin() as conn:
        conn.execute(sql)


def staging_to_target():
    """
    Transfers data from the staging table to the target table. If there are keys already there
        (ticker name, recording date), it updates the previously recorded data.

    :return:
    """
    engine = create_engine(DATA_DB_CONNECTION_STRING)

    sql = """INSERT INTO currencies
SELECT ticker_name, ticker_timestamp, ticker_value
FROM currencies_staging
ON CONFLICT (ticker_name, ticker_timestamp) DO
UPDATE SET ticker_value = excluded.ticker_value"""

    with engine.begin() as conn:
        conn.execute(sql)
