# PostgreSQL connection utilities

Reusable PostgreSQL client, schema-synchronization, query, and loading utilities for the Airflow ELT framework.

## Python requirements

```text
psycopg2-binary
sqlalchemy
pandas
pyyaml
apache-airflow-providers-postgres
```

## Package structure

```text
include/connections/postgres/
├── client.py
└── endpoints/
    ├── select_reader.py
    ├── sync.py
    ├── update_executor.py
    └── writer.py
```

## Airflow connection

Example environment variable:

```env
AIRFLOW_CONN_DW_POSTGRES=postgresql://<username>:<password>@dw_postgres:5432/dw_hr
```

## Usage

```python
from include.connections.postgres.endpoints.sync import PostgresSync
from include.connections.postgres.endpoints.writer import PostgresLoader

contract = "include/elt/config/src/src_hr_employees.yml"

PostgresSync(contract, conn_id="dw_postgres").sync()
PostgresLoader(contract, conn_id="dw_postgres").load(df)
```
