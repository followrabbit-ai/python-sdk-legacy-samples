"""Mechanism 3 (FALLBACK): Airflow seam via the BigQuery hook and operator.

PREFER THE PLUGIN. For Airflow, the recommended approach is Rabbit's official
plugin shown in ``plugin_based_dag.py`` -- it patches ``BigQueryHook.insert_job``
and optimizes every job (hook calls and ``BigQueryInsertJobOperator``, including
``deferrable=True``) transparently, with no DAG code. Use THIS module only when
you cannot install a plugin in the Airflow environment, or when you specifically
want the same explicit ``resolve_reservation`` seam across all three mechanisms.

Do not use both at once: if the plugin is installed, the manual injection here
would re-optimize an already-modified configuration.

This mirrors a typical legacy Airflow usage (a hook-based run-query helper and an
insert-job task builder). Airflow's BigQuery abstractions run queries
through ``jobs.insert`` under the hood, and the reservation goes in
``configuration["reservation"]`` -- the same place as Mechanism 1.

Two integration points are shown:

1. Hook path (``run_query_with_hook``): resolve the reservation BEFORE calling
   ``BigQueryHook.insert_job`` and inject it into the configuration. Use this for
   imperative, non-templated queries built in Python.

2. Operator path (``ReservationAwareBigQueryInsertJobOperator``): a subclass of
   ``BigQueryInsertJobOperator`` that resolves the reservation in ``execute()``.
   This is required because ``configuration.query`` is a TEMPLATED field -- the
   SQL is only rendered at runtime, so the reservation must be resolved then, not
   at DAG-parse time.

This file is a DAG definition meant to run inside Airflow / Cloud Composer 3
(which already ships apache-airflow + apache-airflow-providers-google). It
byte-compiles standalone, but importing it requires Airflow to be installed.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

# Importable from the repo root; in Composer, ship rabbit_reservation.py
# alongside the DAG (e.g. in the dags/ folder or a packaged module).
from rabbit_reservation import resolve_reservation

GCP_CONN_ID = "google_cloud_default"


def run_query_with_hook(query, project_id, **_):
    """Hook path: resolve the reservation before insert_job and inject it."""
    configuration = {
        "query": {
            "query": query,
            "useLegacySql": False,
        }
    }
    reservation = resolve_reservation(
        query, project_id=project_id, process_name="airflow_hook_run_query"
    )
    if reservation:
        configuration["reservation"] = reservation

    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    job = hook.insert_job(
        configuration=configuration,
        project_id=project_id,
        nowait=False,
    )
    return [dict(row) for row in job.result()]


class ReservationAwareBigQueryInsertJobOperator(BigQueryInsertJobOperator):
    """BigQueryInsertJobOperator that resolves a Rabbit reservation at runtime.

    ``configuration.query.query`` is templated, so it is only rendered when the
    task runs. We resolve the reservation in ``execute()`` (after templating) and
    inject it into ``configuration["reservation"]`` before delegating to the base
    operator, which performs the ``jobs.insert``.
    """

    def execute(self, context):
        query_cfg = self.configuration.get("query", {})
        query_str = query_cfg.get("query", "")
        reservation = resolve_reservation(
            query_str,
            project_id=getattr(self, "project_id", None),
            process_name="ReservationAwareBigQueryInsertJobOperator",
        )
        if reservation:
            self.configuration["reservation"] = reservation
        return super().execute(context)


with DAG(
    dag_id="reservation_aware_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rabbit", "bigquery", "reservation"],
) as dag:
    # Operator path: the query is templated and resolved at execute() time.
    insert_job = ReservationAwareBigQueryInsertJobOperator(
        task_id="insert_job_with_reservation",
        gcp_conn_id=GCP_CONN_ID,
        deferrable=True,
        configuration={
            "query": {
                "query": "SELECT '{{ ds }}' AS run_date, 1 AS answer",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )
