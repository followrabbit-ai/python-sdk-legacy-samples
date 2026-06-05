# Rabbit reservation integration for legacy BigQuery access patterns

This repo demonstrates integrating the [Rabbit Dynamic Pricing](https://followrabbit.ai)
BigQuery Job Optimizer into a pre-existing codebase that submits BigQuery query jobs through
**legacy access patterns**. It implemented a single seam to integrate with Rabbit,
`resolve_reservation(...)`, and this repo shows how that seam can be built out and completed.

The three submission mechanisms it targets are described in [The three mechanisms and where the reservation goes](#the-three-mechanisms-and-where-the-reservation-goes) below.

## The seam: `resolve_reservation`

A legacy codebase's BigQuery handlers call a stub:

```python
def resolve_reservation(query_str, project_id=None, process_name=None):
    """Return 'projects/{project}/locations/{location}/reservations/{id}'
    to run the query under that reservation, or None to run on-demand."""
```

[`rabbit_reservation.py`](rabbit_reservation.py) supplies the body: it sends the
query to Rabbit's optimizer, which picks the most cost-effective reservation (or
on-demand) per job, and returns the chosen reservation path. The optimizer
client is **built once and cached**, and the seam **fails open** -- any error
returns `None` so the query still runs on-demand and the hot path never breaks.

## Legacy-pattern caveat

This repo integrates with an existing legacy stack as-is; it is not a
recommendation for new code:

- Mechanisms 1 and 2 use the [`google-api-python-client`](https://github.com/googleapis/google-api-python-client)
  discovery client, which Google lists as in **maintenance mode**. The
  recommended library for new code is [`google-cloud-bigquery`](https://cloud.google.com/python/docs/reference/bigquery/latest).
- The hot path defaults to BigQuery **Legacy SQL** (`useLegacySql=True`) and sets
  the deprecated `maximumBillingTier`.

(A modern `google-cloud-bigquery` equivalent -- a drop-in `bigquery.Client`
subclass whose `query()` is optimized by Rabbit -- is
[also available from Rabbit](https://github.com/followrabbit-ai/python-sdk-samples)
if your codebase uses the recommended client.)

## The three mechanisms and where the reservation goes

| Mechanism | File | Reservation location |
|-----------|------|----------------------|
| 1. `jobs.insert` + poll (dominant) | [`jobs_insert_demo.py`](jobs_insert_demo.py) | `configuration.reservation` in the Job body |
| 2. `jobs.query` (synchronous) | [`jobs_query_demo.py`](jobs_query_demo.py) | top-level `reservation` in the QueryRequest |
| 3. Airflow | [`airflow/`](airflow/README.md) (plugin primary; seam fallback) | `configuration["reservation"]` |

Notes:

- **Mechanism 1** also includes an optional `optimize_job_data_full(...)` variant
  that sends the *entire* job configuration through Rabbit (not just the
  reservation). This higher-fidelity option is unique to the insert path because
  the discovery client is dict-native -- `configuration` already IS the REST Job
  configuration the SDK optimizes.
- **Mechanism 2** is reservation-only by construction: a QueryRequest is not a
  Job resource, so it can only carry the top-level `reservation`.
- **Mechanism 3 (Airflow)** is best handled by Rabbit's
  [official Airflow plugin](https://github.com/followrabbit-ai/bq-job-optimizer-airflow-plugin),
  which patches `BigQueryHook.insert_job` and optimizes every job transparently
  (hook calls and `BigQueryInsertJobOperator`, including `deferrable=True`) with
  no DAG code. See [`airflow/README.md`](airflow/README.md). The explicit
  `resolve_reservation` seam in
  [`airflow/no_plugin_fallback_dag.py`](airflow/no_plugin_fallback_dag.py) is kept
  only as a fallback for environments that can't install a plugin. The plugin
  affects only Airflow; Mechanisms 1 and 2 still use the seam.

## Configuration (environment variables)

| Variable | Read by | Meaning |
|----------|---------|---------|
| `RABBIT_API_KEY` | SDK | Rabbit API key (required to call the optimizer) |
| `RABBIT_API_BASE_URL` | SDK | Optional override; defaults to the production endpoint |
| `RABBIT_RESERVATION_IDS` | seam | Comma-separated **candidate** reservation IDs Rabbit chooses among. Empty -> the seam returns `None` (always on-demand) |
| `RABBIT_DEFAULT_PRICING_MODE` | seam | `on_demand` (default) or `slot_based` |
| `RABBIT_OPTIMIZE_TIMEOUT` | seam | Optional seconds (float) for the SDK request timeout; tighten below the 5s default for the hot path |

Each entry in `RABBIT_RESERVATION_IDS` encodes its region (e.g.
`my-project:US.my-reservation-us`), so Rabbit selects a same-region reservation
and the seam needs no separate location parameter.

## Install and run

```bash
pip install -r requirements.txt

export RABBIT_API_KEY="..."
export RABBIT_RESERVATION_IDS="my-project:US.my-reservation-us,my-project:EU.my-reservation-eu"

# Dry run: resolve + print the request body, no GCP calls
python jobs_insert_demo.py --project-id my-project --dry-run
python jobs_query_demo.py  --project-id my-project --dry-run

# Live run (requires GCP credentials, e.g. GOOGLE_APPLICATION_CREDENTIALS)
python jobs_insert_demo.py --project-id my-project --query "SELECT 1"
python jobs_query_demo.py  --project-id my-project --query "SELECT 1"
```

For Airflow (Mechanism 3), see [`airflow/README.md`](airflow/README.md): the
recommended path is Rabbit's official plugin (no DAG code), with the explicit
seam DAG kept as a fallback. Both are meant to run inside Airflow / Cloud
Composer 3 (which already ships `apache-airflow` and
`apache-airflow-providers-google`); for the fallback seam DAG, ship
`rabbit_reservation.py` alongside it so it is importable.

## Open questions (defaults applied, confirm for your environment)

These are integration decisions where this repo applies a sensible default. The
defaults work for the common case, but each is worth confirming against your
own environment:

1. **Region / location.** Handled implicitly via region-encoded
   `RABBIT_RESERVATION_IDS`, so no location is passed. Confirm whether any
   workload needs an explicit location instead.
2. **Candidate reservation source.** Provided via env (`RABBIT_RESERVATION_IDS`)
   and constant per process. `process_name` is used only client-side for
   logging; confirm whether candidates should vary by workload.
3. **Latency / fail-open.** `resolve_reservation` adds one synchronous Rabbit
   call per query on the hot path. It uses a short timeout and fails open to
   on-demand. Confirm the acceptable added per-query latency.
