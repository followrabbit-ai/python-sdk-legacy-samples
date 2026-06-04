"""Mechanism 2: ``jobs.query`` (synchronous "submit and wait").

This mirrors the non-wait branch of a synchronous query handler:
the legacy ``google-api-python-client`` discovery client calls
``jobs.query``, which creates the job, waits server-side, and returns rows inline
in one response.

The reservation goes at the TOP LEVEL of the QueryRequest body
(``query_body["reservation"]``), not under ``configuration`` -- a QueryRequest is
not a Job resource. Because of that, this path is reservation-only by
construction: there is no full job-config optimization variant here.

Run modes::

    python jobs_query_demo.py --project-id my-proj --dry-run   # resolve + print
    python jobs_query_demo.py --project-id my-proj             # actually submit

The module is import-safe: ``google-api-python-client`` is imported lazily.
"""

from __future__ import annotations

import argparse

from rabbit_reservation import resolve_reservation

PROCESS_NAME = "jobs_query_demo"

DEFAULT_QUERY = "SELECT 1 AS answer"


def build_query_body(query_str, project_id):
    """Build the QueryRequest body and inject a Rabbit reservation if any."""
    query_body = {"query": query_str}
    reservation = resolve_reservation(
        query_str, project_id=project_id, process_name=PROCESS_NAME
    )
    if reservation:
        query_body["reservation"] = reservation
    return query_body, reservation


def execute_query(bigquery_service, query_str, project_id):
    """Submit synchronously via jobs.query; rows are returned inline."""
    query_body, _ = build_query_body(query_str, project_id)
    return (
        bigquery_service.jobs()
        .query(projectId=project_id, body=query_body)
        .execute()
    )


def _build_service():
    """Build the legacy discovery client (imported lazily for import-safety)."""
    from googleapiclient.discovery import build

    return build("bigquery", "v2")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", required=True, help="GCP project ID")
    parser.add_argument("--query", default=DEFAULT_QUERY, help="SQL to run")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve the reservation and print the request without submitting",
    )
    args = parser.parse_args()

    if args.dry_run:
        query_body, reservation = build_query_body(args.query, args.project_id)
        print(f"Resolved reservation: {reservation or 'on-demand'}")
        print(f"query_body = {query_body}")
        return

    service = _build_service()
    result = execute_query(service, args.query, args.project_id)
    print(result)


if __name__ == "__main__":
    main()
