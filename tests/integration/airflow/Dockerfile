FROM python:3.12-slim
ARG AIRFLOW_VERSION
# google-re2 and other binary wheels require the C++ runtime on the slim base.
RUN apt-get update \
    && apt-get install --no-install-recommends -y libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
ENV AIRFLOW_HOME=/opt/airflow \
    AIRFLOW__CORE__LOAD_EXAMPLES=false \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/tests/dags \
    AIRFLOW__CORE__EXECUTOR=SequentialExecutor \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db \
    AIRFLOW__CORE__FERNET_KEY="" \
    AIRFLOW__CORE__UNIT_TEST_MODE=true \
    AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120 \
    PYTHONUNBUFFERED=1
COPY requirements.txt /tmp/requirements.txt
COPY dist/ /tmp/dist/
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt \
    && python -m pip install --no-cache-dir --no-deps /tmp/dist/*.whl \
    && python -m pip check \
    && python -c "from importlib.metadata import version; assert version('apache-airflow') == '${AIRFLOW_VERSION}'" \
    && useradd --uid 50000 --create-home airflow \
    && mkdir -p /opt/airflow/tests \
    && chown -R airflow:airflow /opt/airflow
COPY --chown=airflow:airflow dags/ /opt/airflow/tests/dags/
COPY --chown=airflow:airflow run_suite.py /opt/airflow/tests/run_suite.py
USER airflow
WORKDIR /opt/airflow
CMD ["python", "-I", "/opt/airflow/tests/run_suite.py"]
