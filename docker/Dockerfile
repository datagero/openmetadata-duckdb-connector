FROM openmetadata/ingestion:1.5.0

# Let's use the same workdir as the ingestion image
WORKDIR ingestion
USER airflow

# Install our custom connector
COPY duckdb-connector/connector connector
COPY duckdb-connector/setup.py .

# For now, manually copy database files to the container
COPY dev.duckdb /data/duckdb/dev.duckdb

# Install the duckdb Python package
RUN pip install duckdb

RUN pip install --no-deps .