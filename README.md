# ETL Test Case

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/apache-airflow.svg)](https://pypi.org/project/apache-airflow/)

**Table of contents**

- [General information](#general-information)
- [Build Docker image](#build-docker-image)
- [Run Docker Compose](#run-docker-compose)
- [DAGs](#dags)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## General information
The scrapper collects current and historical data for individual tickers using the public API https://exchangerate.host/

Work with several tickers at once is supported, examples of configurations are given in [dags/history_download.py]() and [dags/latest_download.py]()

## Build Docker image

In order to make sure that the versions of the libraries we need are used in the airflow image, before starting the docker, you need to run the command in the root directory

- docker-compose build

## Run Docker Compose

To start docker compose, run the following command:

- docker compose up

This command creates several containers:
- airflow-webserver - container with airflow itself;
- airflow-scheduler - airflow scheduler;
- postgres - PostgreSQL DB for storing airflow metadata;
- postgred-data - PostgreSQL DB in which the data of interest to us on tickets will be stored

## DAGs

By default, there are two dags in the airflow:
- history_download (execution schedule - one-time) - dag that downloads all available data for this ticker in the specified time period by the provided ticker name, as well as the left and right borders of the time interval. Since there is a restriction in the API that one request can contain data for one year, the time interval is divided into several intervals, requests for which go asynchronously
- latest_download (execution schedule - every 3 hours) - dag that one-time downloads the last known value of the specified ticker.