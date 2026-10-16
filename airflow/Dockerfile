# Custom Airflow image: official image + extra Python packages from requirements.txt
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}

# Re-declared: ARGs before FROM are not visible after it
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION=3.13

COPY requirements.txt /tmp/requirements.txt

# Airflow's constraints file keeps its own pinned dependencies from being
# upgraded/broken by the extra packages. Packages not in the constraints file
# (your own) are unconstrained. Remove the -c flag if a pin conflicts.
RUN pip install --no-cache-dir \
      -c "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      -r /tmp/requirements.txt
