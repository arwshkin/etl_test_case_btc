FROM apache/airflow:2.3.0
COPY python_requirements.txt .
RUN pip install -r python_requirements.txt