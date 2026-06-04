"""Rabbit-backed implementation of the ``resolve_reservation`` seam.

This stub is already present in the target BigQuery handlers with a fixed
contract; this module supplies the body. Given the SQL text, it asks the Rabbit
Dynamic Pricing optimizer which reservation (if any) the job should run under
and returns the reservation resource path, or ``None`` to run on-demand.

Design notes:

- Reservation-only by design. The stub returns a single reservation
  path, so this seam carries only the ``reservation_assignment`` optimization.
  (Full job-config optimization is shown separately in ``jobs_insert_demo.py``.)
- Fail-open. Any error -- missing API key, network timeout, malformed response
  -- results in ``None`` so the query still runs on-demand. The hot path must
  never break because the optimizer is unreachable.
- The optimizer client is built once and cached; ``resolve_reservation`` is on a
  high-volume path and must not construct a new client per call.

Configuration is read from the environment so it can live behind the seam (the
stub signature does not carry credentials or candidate reservations):

- ``RABBIT_API_KEY`` / ``RABBIT_API_BASE_URL`` -- read by the SDK itself.
- ``RABBIT_RESERVATION_IDS`` -- comma-separated candidate reservation IDs Rabbit
  may choose among. Empty -> the seam returns ``None`` (always on-demand). Each
  ID encodes its region (e.g. ``my-project:US.my-reservation-us``), so Rabbit
  selects a same-region reservation and the seam needs no location parameter.
- ``RABBIT_DEFAULT_PRICING_MODE`` -- ``on_demand`` (default) or ``slot_based``.
- ``RABBIT_OPTIMIZE_TIMEOUT`` -- optional seconds (float) for the SDK request
  timeout; tighten below the SDK default of 5s for the hot path.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

VALID_PRICING_MODES = ("on_demand", "slot_based")

_optimizer = None


def _candidate_reservation_ids():
    """Parse ``RABBIT_RESERVATION_IDS`` into a list, dropping blanks."""
    raw = os.environ.get("RABBIT_RESERVATION_IDS", "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _default_pricing_mode():
    mode = os.environ.get("RABBIT_DEFAULT_PRICING_MODE", "on_demand").strip()
    if mode not in VALID_PRICING_MODES:
        logger.warning(
            "Rabbit: invalid RABBIT_DEFAULT_PRICING_MODE %r; falling back to "
            "'on_demand'. Valid values: %s",
            mode,
            ", ".join(VALID_PRICING_MODES),
        )
        return "on_demand"
    return mode


def _get_optimizer():
    """Build and cache the Rabbit optimizer client.

    Imported lazily so this module is import-safe without the SDK installed and
    so a missing API key only matters when optimization is actually attempted.
    """
    global _optimizer
    if _optimizer is None:
        from rabbit_bq_job_optimizer import RabbitBQJobOptimizer

        kwargs = {}
        timeout = os.environ.get("RABBIT_OPTIMIZE_TIMEOUT")
        if timeout:
            kwargs["timeout"] = float(timeout)
        # api_key / base_url left unset so the SDK reads RABBIT_API_KEY /
        # RABBIT_API_BASE_URL (or its default base URL) itself.
        _optimizer = RabbitBQJobOptimizer(**kwargs)
    return _optimizer


def resolve_reservation(query_str, project_id=None, process_name=None):
    """Return the BigQuery reservation resource path for a query, or None.

    Args:
        query_str:    the SQL text about to be executed.
        project_id:   the GCP project the job will run in (optional).
        process_name: a label identifying the workload (optional).

    Returns:
        The full reservation resource path
        'projects/{project}/locations/{location}/reservations/{id}'
        to run the query under that reservation, or None to run on-demand.
    """
    reservation_ids = _candidate_reservation_ids()
    if not reservation_ids:
        # No candidates configured: nothing for Rabbit to choose among.
        return None

    try:
        from rabbit_bq_job_optimizer import OptimizationConfig

        optimizer = _get_optimizer()
        configuration = {"configuration": {"query": {"query": query_str}}}
        result = optimizer.optimize_job(
            configuration=configuration,
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
        # Rabbit returns the chosen reservation under
        # optimizedJob["configuration"]["reservation"], in the same
        # projects/{p}/locations/{loc}/reservations/{id} format the stub
        # promises, and omits it when on-demand is selected.
        reservation = result.optimizedJob["configuration"].get("reservation")
        logger.info(
            "Rabbit: resolve_reservation(process_name=%r) -> %s",
            process_name,
            reservation or "on-demand",
        )
        return reservation
    except Exception as exc:  # noqa: BLE001 - hot path must fail open
        logger.warning(
            "Rabbit: optimization failed (%s); running on-demand.", exc
        )
        return None
