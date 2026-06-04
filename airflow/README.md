# Mechanism 3: Airflow

There are two ways to get Rabbit reservation optimization into Airflow. For most
deployments the **plugin is the right choice**.

## Primary: the official Rabbit plugin (recommended)

File: [`plugin_based_dag.py`](plugin_based_dag.py)

Rabbit's [official Airflow 2 plugin](https://github.com/followrabbit-ai/bq-job-optimizer-airflow-plugin)
monkey-patches `BigQueryHook.insert_job`. Every BigQuery operator and hook call
submits jobs through `insert_job`, so the plugin optimizes them all
transparently -- **including `BigQueryInsertJobOperator` (even `deferrable=True`)
and direct `hook.insert_job(...)`** -- with no changes to your DAGs. It sends the
full job configuration through Rabbit (higher fidelity than reservation-only)
and fails open to the original configuration on any error.

Setup (one time, per environment):

1. Install the SDK: `pip install rabbit-bq-job-optimizer` (already in
   [`../requirements.txt`](../requirements.txt)).
2. Deploy `rabbit_bq_optimizer_plugin.py` to `$AIRFLOW_HOME/plugins/` (on Cloud
   Composer, the environment bucket's `plugins/` folder) and restart the
   scheduler/webserver/workers.
3. Create the `rabbit_api` **connection** (API key in the password field):

```bash
airflow connections add rabbit_api \
    --conn-type generic \
    --conn-password "<your-rabbit-api-key>" \
    --conn-extra '{"api_base_url": "https://api.followrabbit.ai/bq-job-optimizer"}'
```

4. Set the `rabbit_bq_optimizer_config` **variable** (reservation IDs use the
   `project:region.reservation-name` form):

```bash
airflow variables set rabbit_bq_optimizer_config '{
    "default_pricing_mode": "on_demand",
    "reservation_ids": [
        "my-project:US.my-reservation-us",
        "my-project:EU.my-reservation-eu"
    ]
}'
```

## Fallback: the explicit `resolve_reservation` seam

File: [`no_plugin_fallback_dag.py`](no_plugin_fallback_dag.py)

Use this only when you **cannot install a plugin** in the Airflow environment, or
when you want the same explicit `resolve_reservation(...)` seam across all three
mechanisms. It shows a hook path (`run_query_with_hook`) that resolves before
`insert_job`, and a `ReservationAwareBigQueryInsertJobOperator` that resolves in
`execute()` after templating renders the SQL.

## Caveats / things to confirm

- **Pick one, not both.** If the plugin is enabled, do not also inject a
  reservation in DAG code -- the plugin would re-optimize an already-modified
  config.
- **Airflow 2 only.** The plugin is the Airflow 2 variant. Cloud Composer 3 ships
  Airflow 2.x; confirm compatibility before using on Airflow 3.
- **Global blast radius.** The plugin patches `insert_job` for the whole
  environment (loads/copies/extracts too). Reservation-only optimization plus
  fail-open keep this safe.
- **Deferrable operators.** Job submission still happens via `hook.insert_job` in
  the operator's `execute()` (the trigger only polls completion), so the patch
  applies. Worth a one-time validation in your version.

## Scope note

The plugin only affects Airflow (Mechanism 3). The non-Airflow hot paths --
Mechanism 1 (`jobs.insert` via the discovery client) and Mechanism 2
(`jobs.query`) -- are not touched by the plugin and still use the
`resolve_reservation` seam in [`../rabbit_reservation.py`](../rabbit_reservation.py).
