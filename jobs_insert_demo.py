"""Mechanism 1: ``jobs.insert`` + polling (the dominant legacy hot path).

This mirrors a high-volume query handler: the job is submitted
asynchronously through the legacy ``google-api-python-client``
discovery client, then ``jobs.getQueryResults`` is polled until ``jobComplete``.
The Rabbit reservation is injected into ``configuration.reservation`` of the Job
body, exactly where ``jobs.insert`` expects it.

Two variants are shown:

1. ``build_job_data`` -- reservation-only, matching the stub usage: ask
   ``resolve_reservation`` for a path and set ``configuration.reservation``.
2. ``optimize_job_data_full`` -- the higher-fidelity option unique to the insert
   path: because the discovery client is dict-native, ``configuration`` IS the
   REST Job configuration the SDK optimizes, so the whole config can be sent
   through Rabbit and merged back (no to_api_repr / from_api_repr bridging that
   the modern google-cloud-bigquery client needs).

Run modes::

    python jobs_insert_demo.py --dry-run   # resolve + print, no API calls
    python jobs_insert_demo.py             # actually submit (needs GCP creds)

The module is import-safe: ``google-api-python-client`` is imported lazily so the
file parses and ``--dry-run`` works without credentials or the library.
"""

from __future__ import annotations

import argparse
import time

from rabbit_reservation import resolve_reservation

PROCESS_NAME = "jobs_insert_demo"

DEFAULT_QUERY = "SELECT 1 AS answer"


def build_job_data(query_str, project_id, use_legacy_sql=True):
    """Build the Job resource body and inject a Rabbit reservation if any.

    Reservation is opt-in and fail-safe: it is added only when the resolver
    returns one, leaving the job on-demand otherwise.
    """
    job_data = {
        "jobReference": {"projectId": project_id},
        "configuration": {
            "query": {
                "query": query_str,
                "maximumBillingTier": 2,
                "priority": "INTERACTIVE",
                "useLegacySql": use_legacy_sql,
            }
        },
    }
    reservation = resolve_reservation(
        query_str, project_id=project_id, process_name=PROCESS_NAME
    )
    if reservation:
        job_data["configuration"]["reservation"] = reservation
    return job_data, reservation


def optimize_job_data_full(job_data):
    """Optional insert-only variant: optimize the whole Job configuration.

    Sends the complete ``configuration`` dict through Rabbit and merges the
    optimized configuration back. Fails open: on any error the original
    ``job_data`` is returned unchanged.
    """
    try:
        from rabbit_bq_job_optimizer import OptimizationConfig

        from rabbit_reservation import (
            _candidate_reservation_ids,
            _default_pricing_mode,
            _get_optimizer,
        )

        reservation_ids = _candidate_reservation_ids()
        if not reservation_ids:
            return job_data

        optimizer = _get_optimizer()
        result = optimizer.optimize_job(
            configuration={"configuration": job_data["configuration"]},
            enabledOptimizations=[
                OptimizationConfig(
                    type="reservation_assignment",
                    config={
                        "defaultPricingMode": _default_pricing_mode(),
                        "reservationIds": reservation_ids,
                    },
                )
            ],
        )
        job_data["configuration"] = result.optimizedJob["configuration"]
        return job_data
    except Exception:  # noqa: BLE001 - mirror the seam's fail-open behavior
        return job_data


def execute_query(bigquery_service, query_str, project_id, use_legacy_sql=True):
    """Submit via jobs.insert and poll jobs.getQueryResults until complete."""
    job_data, _ = build_job_data(query_str, project_id, use_legacy_sql)

    response = (
        bigquery_service.jobs()
        .insert(projectId=project_id, body=job_data)
        .execute()
    )
    job_id = response["jobReference"]["jobId"]

    response = (
        bigquery_service.jobs()
        .getQueryResults(projectId=project_id, jobId=job_id)
        .execute()
    )
    while not response["jobComplete"]:
        time.sleep(1)
        response = (
            bigquery_service.jobs()
            .getQueryResults(projectId=project_id, jobId=job_id)
            .execute()
        )
    return response


def _build_service():
    """Build the legacy discovery client (imported lazily for import-safety)."""
    from googleapiclient.discovery import build

    return build("bigquery", "v2")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", required=True, help="GCP project ID")
    parser.add_argument("--query", default=DEFAULT_QUERY, help="SQL to run")
    parser.add_argument(
        "--use-legacy-sql",
        action="store_true",
        help="Use BigQuery Legacy SQL (matches the legacy hot path default)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve the reservation and print the job body without submitting",
    )
    args = parser.parse_args()

    if args.dry_run:
        job_data, reservation = build_job_data(
            args.query, args.project_id, args.use_legacy_sql
        )
        print(f"Resolved reservation: {reservation or 'on-demand'}")
        print(f"job_data = {job_data}")
        return

    service = _build_service()
    result = execute_query(
        service, args.query, args.project_id, args.use_legacy_sql
    )
    print(result)


if __name__ == "__main__":
    main()
