"""
Your defense. Implement register(ctx) and a handler per event type.
See ../README.md for the full interface + toolkit reference, and
../RULES.md before you start.
"""

import collections

from api import Verdict

# Every ctx.baseline limit is a clean-stream mean ± 3σ. A missed fault costs
# ~4x more score than a false alarm (50/n_faulty vs 30/n_clean) and subtle
# faults sit just past normal variance, so we alert inside 3σ. Both cutoffs
# give the same ~0.6% false-alarm rate per metric: one tail past 2.5σ, or
# either tail past 2.75σ.
Z_ONE_SIDED = 2.5
Z_TWO_SIDED = 2.75

# Clean samples needed before the running median is trusted as the mean.
# The one-sided cutoff is mean + 2.5·(cap − mean)/3 = cap − (cap − mean)/6,
# so an error in the mean moves it by only 1/6: a median of 3 is plenty, and
# a longer warm-up leaves early subtle faults judged at the raw 3σ cap.
WARMUP = 3

# data_batch composite: sum of squared z over 4 metrics ~ χ²(4), p ≈ 0.01.
COMPOSITE_LIMIT = 13.28

# The published feature_mean_shift_sigma_max (0.41) understates natural
# serving variance: clean batches reach ~0.47 while real skew starts ~0.97.
FEATURE_SHIFT_LIMIT = 0.65

# Upstreams a model can't be built without (dbt graph from the lab brief).
REQUIRED_UPSTREAM = {"stg_orders": {"raw.orders", "raw.customers"}}


def _verdict(alert, pillar, reason=None):
    if reason is not None:
        return Verdict(alert=alert, pillar=pillar, reason=reason)
    return Verdict(alert=alert, pillar=pillar)


def _tool_error(result):
    return not isinstance(result, dict) or "error" in result


def _number(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _median(values):
    # statistics.median is off-limits: the sandbox blocks its `numbers` import.
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _clean_history(ctx, name):
    return ctx.state.setdefault("clean", {}).setdefault(name, [])


def _remember_clean(ctx, values):
    for name, value in values.items():
        _clean_history(ctx, name).append(value)


def _zscore(ctx, name, value, hi, lo=None):
    """z of value against the clean distribution behind a mean±3σ baseline.

    A two-sided baseline gives mean and σ directly. A one-sided cap only
    gives mean + 3σ, so its mean is the running median of values seen on
    events judged clean, and None is returned until that has warmed up.
    """
    if lo is not None:
        return (value - (lo + hi) / 2) / ((hi - lo) / 6)

    history = _clean_history(ctx, name)
    if len(history) < WARMUP:
        return None
    center = _median(history)
    if center >= hi:
        return None
    return (value - center) / ((hi - center) / 3)


def _anomalous(ctx, name, value, hi, lo=None):
    """(alert, z). One-sided caps use the raw baseline until warmed up."""
    z = _zscore(ctx, name, value, hi, lo)
    if lo is not None:
        return abs(z) > Z_TWO_SIDED, z
    if z is None:
        return value > hi, None
    return z > Z_ONE_SIDED, z


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def check_data_batch(payload, ctx):
    pillar = "checks"
    baseline = ctx.baseline

    result = ctx.tools.batch_profile(payload["batch_id"])
    if _tool_error(result):
        return _verdict(False, pillar)

    values = {
        "row_count": _number(result.get("row_count")),
        "null_rate": _number(
            (result.get("null_rate") or {}).get("customer_id")
        ),
        "mean_amount": _number(result.get("mean_amount")),
        "staleness_min": _number(result.get("staleness_min")),
    }
    if None in values.values():
        return _verdict(False, pillar)

    # (metric, upper limit, lower limit or None if one-sided, reason)
    limits = [
        ("row_count", baseline["row_count_max"],
         baseline["row_count_min"], "row_count outside clean range"),
        ("null_rate", baseline["null_rate_max"],
         None, "customer_id null rate too high"),
        ("mean_amount", baseline["mean_amount_max"],
         baseline["mean_amount_min"], "mean_amount outside clean range"),
        ("staleness_min", baseline["staleness_min_max"],
         None, "data freshness lag"),
    ]

    reasons = []
    zs = []
    for name, hi, lo, reason in limits:
        alert, z = _anomalous(ctx, name, values[name], hi, lo)
        if alert:
            reasons.append(reason)
        if z is not None:
            # Only the bad direction counts for one-sided metrics.
            zs.append(z if lo is not None else max(z, 0.0))

    # Several metrics drifting together while each stays under its limit.
    if len(zs) == len(limits):
        distance = sum(z * z for z in zs)
        if distance > COMPOSITE_LIMIT:
            reasons.append(
                f"multi-signal composite anomaly (chi2={distance:.1f})"
            )

    if reasons:
        return _verdict(True, pillar, "; ".join(reasons))

    _remember_clean(ctx, {
        "null_rate": values["null_rate"],
        "staleness_min": values["staleness_min"],
    })
    return _verdict(False, pillar)


def check_contract_checkpoint(payload, ctx):
    pillar = "contracts"

    result = ctx.tools.contract_diff(
        payload["contract_id"],
        payload["checkpoint_batch_id"],
    )
    if _tool_error(result):
        return _verdict(False, pillar)

    reasons = []

    violations = result.get("violations") or []
    if violations:
        reasons.append(
            "contract violations: " + ", ".join(map(str, violations))
        )

    delay = _number(result.get("freshness_delay_min"))
    if delay is not None:
        declared_sla = payload.get("declared_sla") or {}
        sla_limit = _number(declared_sla.get("freshness_min"))

        if sla_limit is not None and delay > sla_limit:
            reasons.append("freshness SLA violated")
        elif _anomalous(
            ctx, "freshness_delay_min", delay,
            ctx.baseline["freshness_delay_max_min"],
        )[0]:
            reasons.append("freshness delay unusually high")

    if reasons:
        return _verdict(True, pillar, "; ".join(reasons))

    if delay is not None:
        _remember_clean(ctx, {"freshness_delay_min": delay})
    return _verdict(False, pillar)


def _usual_upstream(ctx, job):
    """Upstreams present in most clean runs of this job seen so far."""
    runs = ctx.state.get("upstream_runs", {}).get(job, [])
    if len(runs) < WARMUP:
        return set()
    counts = collections.Counter(item for run in runs for item in run)
    return {item for item, n in counts.items() if n > len(runs) / 2}


def check_lineage_run(payload, ctx):
    pillar = "lineage"

    result = ctx.tools.lineage_graph_slice(payload["run_id"])
    if _tool_error(result):
        return _verdict(False, pillar)

    reasons = []

    downstream = _number(result.get("actual_downstream_count"))
    if downstream == 0:
        reasons.append("orphan output: no downstream consumers")

    upstream = result.get("actual_upstream") or []
    if isinstance(upstream, str):
        upstream = [upstream]
    upstream = {str(item).lower() for item in upstream}

    # The payload names the dbt model in `job`, e.g. "dbt:stg_orders".
    job = str(payload.get("job") or payload.get("model") or "").lower()
    model = job.split(":")[-1]
    expected = REQUIRED_UPSTREAM.get(model, set()) | _usual_upstream(ctx, job)
    missing = expected - upstream
    if missing:
        reasons.append("missing upstream: " + ", ".join(sorted(missing)))

    duration = _number(result.get("duration_ms"))
    if duration is not None and _anomalous(
        ctx, "lineage_duration_ms", duration,
        ctx.baseline["lineage_duration_ms_max"],
    )[0]:
        reasons.append("lineage runtime anomaly")

    if reasons:
        return _verdict(True, pillar, "; ".join(reasons))

    ctx.state.setdefault("upstream_runs", {}).setdefault(job, []).append(
        upstream
    )
    if duration is not None:
        _remember_clean(ctx, {"lineage_duration_ms": duration})
    return _verdict(False, pillar)


def check_feature_materialization(payload, ctx):
    pillar = "ai_infra"

    result = ctx.tools.feature_drift(
        payload["feature_view"],
        payload["batch_id"],
    )
    if _tool_error(result):
        return _verdict(False, pillar)

    shift = _number(result.get("mean_shift_sigma"))
    if shift is None:
        return _verdict(False, pillar)

    if shift > FEATURE_SHIFT_LIMIT:
        return _verdict(
            True, pillar,
            f"feature drift: mean_shift_sigma={shift:.4f}",
        )

    return _verdict(False, pillar)


def check_embedding_batch(payload, ctx):
    pillar = "ai_infra"
    baseline = ctx.baseline

    result = ctx.tools.embedding_drift(
        payload["corpus"],
        payload["chunk_batch_id"],
    )
    if _tool_error(result):
        return _verdict(False, pillar)

    shift = _number(result.get("centroid_shift"))
    age = _number(result.get("avg_doc_age_days"))

    if shift is None or age is None:
        return _verdict(False, pillar)

    reasons = []

    if _anomalous(
        ctx, "embedding_centroid_shift", shift,
        baseline["embedding_centroid_shift_max"],
    )[0]:
        reasons.append("embedding centroid drift")

    if _anomalous(
        ctx, "corpus_avg_doc_age_days", age,
        baseline["corpus_avg_doc_age_days_max"],
    )[0]:
        reasons.append("corpus documents are stale")

    if reasons:
        return _verdict(True, pillar, "; ".join(reasons))

    _remember_clean(ctx, {
        "embedding_centroid_shift": shift,
        "corpus_avg_doc_age_days": age,
    })
    return _verdict(False, pillar)
