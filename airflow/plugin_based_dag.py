"""Mechanism 3 (PRIMARY): Airflow via Rabbit's official plugin -- zero DAG code.

This is the recommended way to optimize BigQuery reservations in Airflow. Rabbit
ships an official Airflow 2 plugin
(github.com/followrabbit-ai/bq-job-optimizer-airflow-plugin) that monkey-patches
``BigQueryHook.insert_job``. Because every BigQuery operator and hook call --
including ``BigQueryInsertJobOperator`` (even ``deferrable=True``) and direct
``hook.insert_job(...)`` -- submits jobs through ``insert_job``, the plugin
optimizes them all transparently with NO changes to your DAGs.

Note the DAG below contains no Rabbit code at all: it is a plain
``BigQueryInsertJobOperator``. The optimization happens entirely in the patched
hook. The plugin sends the full job configuration through Rabbit (higher
fidelity than reservation-only) and fails open to the original configuration on
any error.

Setup (one time, per Airflow environment)
-----------------------------------------
1. Install the SDK in the Airflow environment (already in this repo's
   requirements.txt)::

       pip install rabbit-bq-job-optimizer

2. Deploy the plugin file to the Airflow plugins directory (on Cloud Composer
   this is the ``plugins/`` folder of the environment bucket)::

       cp rabbit_bq_optimizer_plugin.py $AIRFLOW_HOME/plugins/

   then restart the scheduler / webserver / workers so the patch loads.

3. Store the Rabbit API key in an Airflow CONNECTION named ``rabbit_api``
   (the key goes in the password field; optional base-URL override in extras)::

       airflow connections add rabbit_api \\
           --conn-type generic \\
           --conn-password "<your-rabbit-api-key>" \\
           --conn-extra '{"api_base_url": "https://api.followrabbit.ai/bq-job-optimizer"}'

4. Store the optimizer parameters in an Airflow VARIABLE named
   ``rabbit_bq_optimizer_config`` (reservation IDs use the
   ``project:region.reservation-name`` form)::

       airflow variables set rabbit_bq_optimizer_config '{
           "default_pricing_mode": "on_demand",
           "reservation_ids": [
               "my-project:US.my-reservation-us",
               "my-project:EU.my-reservation-eu"
           ]
       }'

Important
---------
- Do NOT also inject a reservation in your DAG code when the plugin is enabled;
  the plugin would re-optimize an already-modified config. Use the plugin OR the
  fallback seam (see ``no_plugin_fallback_dag.py``), not both.
- The patch is global: it optimizes every BigQuery ``insert_job`` in the
  environment (loads/copies/extracts too). Reservation-only optimization plus
  fail-open make this safe, but the blast radius is the whole deployment.
- The plugin is the Airflow 2 variant; Cloud Composer 3 ships Airflow 2.x.
  Confirm compatibility before using on Airflow 3.

This file is a DAG definition meant to run inside Airflow / Cloud Composer 3. It
byte-compiles standalone, but importing it requires Airflow to be installed.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

with DAG(
    dag_id="rabbit_plugin_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rabbit", "bigquery", "reservation", "plugin"],
) as dag:
    # No Rabbit code here. With the plugin installed, this stock operator's
    # jobs.insert is optimized transparently by the patched BigQueryHook.
    insert_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        gcp_conn_id="google_cloud_default",
        deferrable=True,
        configuration={
            "query": {
                "query": "SELECT '{{ ds }}' AS run_date, 1 AS answer",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )
