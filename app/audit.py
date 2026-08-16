"""Grade ourselves against PseudoGram's own record.

`GET /v1/simulate/{run_id}/truth` returns exactly what they sent. This module
replays that truth through our own rules and diffs the result against our
ledger, so we can state -- with numbers rather than adjectives -- how many
events never reached us, how many DMs we should have created and didn't, and
whether our duplicates_blocked lines up with theirs.

Everything in FAILURES.md that carries a number came from here.
"""
import time

from . import config, db, matcher, stats
from .sender import client


async def start_simulation(count: int, duration_seconds: int,
                           webhook_url: str | None = None) -> dict:
    url = webhook_url or config.PUBLIC_WEBHOOK_URL
    if not url:
        return {"error": "no webhook_url given and PUBLIC_WEBHOOK_URL unset"}
    resp = await client().post(
        "/v1/simulate/start",
        json={"webhook_url": url, "count": count, "duration_seconds": duration_seconds},
    )
    body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    run_id = body.get("run_id")
    if run_id:
        with db.tx() as conn:
            conn.execute(
                "INSERT INTO sim_runs (run_id, started_at, count, duration) VALUES (?,?,?,?)"
                " ON CONFLICT(run_id) DO NOTHING",
                (run_id, time.time(), count, duration_seconds),
            )
    return {"status": resp.status_code, "body": body, "webhook_url": url}


def _extract_events(truth) -> list[dict]:
    """Pull the event list out of whatever shape the truth file arrives in."""
    if isinstance(truth, list):
        return [e for e in truth if isinstance(e, dict)]
    if not isinstance(truth, dict):
        return []
    for key in ("events", "deliveries", "sent_events", "payloads", "webhooks"):
        value = truth.get(key)
        if isinstance(value, list):
            return [e for e in value if isinstance(e, dict)]
    return []


def _event_fields(event: dict) -> tuple[str, str, str, str, str]:
    """(event_id, event_type, comment_id, user_id, text), tolerant of nesting."""
    data = event.get("data") or event.get("payload") or {}
    if not isinstance(data, dict):
        data = {}
    sender_obj = data.get("from") or event.get("from") or {}
    if not isinstance(sender_obj, dict):
        sender_obj = {}
    return (
        str(event.get("event_id") or data.get("event_id") or ""),
        str(event.get("event_type") or data.get("event_type") or ""),
        str(data.get("comment_id") or event.get("comment_id") or ""),
        str(sender_obj.get("user_id") or data.get("user_id") or event.get("user_id") or ""),
        str(data.get("text") or event.get("text") or ""),
    )


def _audit_summary_truth(run_id: str, truth: dict) -> dict:
    """Audit against PseudoGram's real truth format.

    They report totals and the set of user_ids that should have been DMed --
    not a per-event list. That is *better* than what I originally built for,
    because `webhook_200_count` is their own count of how many deliveries we
    acknowledged. It settles "did you drop anything" from their side of the
    wire, which nothing in my own ledger can do: an event we never received is
    invisible to us by definition.
    """
    attempted = truth.get("total_deliveries_attempted") or 0
    acknowledged = truth.get("webhook_200_count") or 0
    expected = {str(u) for u in (truth.get("expected_unique_recipients") or [])}

    ours = stats.verbose_stats()
    our_recipients = {
        r["user_id"] for r in db.query("SELECT DISTINCT user_id FROM dm_tasks")
    }
    missing = sorted(expected - our_recipients)
    unexpected = sorted(our_recipients - expected)
    unacknowledged = attempted - acknowledged

    return {
        "run_id": run_id,
        "verdict": {
            # Their count of deliveries we failed to 200. This is the number
            # that would have been ~540 while my signature check was wrong, and
            # every stat I report would still have looked perfect.
            "deliveries_we_failed_to_acknowledge": unacknowledged,
            "recipients_we_owe_and_missed": len(missing),
            "recipients_we_dmed_that_truth_doesnt_list": len(unexpected),
            "clean": (unacknowledged == 0 and not missing and not unexpected),
        },
        "truth": {
            "events_generated": truth.get("total_events_generated"),
            "deliveries_attempted": attempted,
            "redeliveries": attempted - (truth.get("total_events_generated") or 0),
            "webhook_200_count": acknowledged,
            "expected_unique_recipients": len(expected),
        },
        "ours": {
            "event_deliveries_recorded": ours["detail"]["event_deliveries"],
            "distinct_events": ours["detail"]["events_distinct"],
            "distinct_recipients": len(our_recipients),
            **{k: ours[k] for k in ("sent", "failed", "queued", "duplicates_blocked")},
            "cancelled_by_delete": ours["detail"]["cancelled_by_delete"],
            "invariants": ours["detail"]["invariants"],
        },
        "samples": {
            "missing_recipients": missing[:10],
            "unexpected_recipients": unexpected[:10],
        },
    }


async def audit_run(run_id: str) -> dict:
    resp = await client().get(f"/v1/simulate/{run_id}/truth")
    if resp.status_code != 200:
        return {"error": "truth_unavailable", "status": resp.status_code,
                "body": resp.text[:500]}
    truth = resp.json()

    # The live API reports summary totals; the local chaos clone reports a full
    # event list. Handle whichever we're pointed at.
    if isinstance(truth, dict) and "expected_unique_recipients" in truth:
        return _audit_summary_truth(run_id, truth)

    events = _extract_events(truth)
    if not events:
        return {"error": "could not parse truth payload", "raw_keys": list(truth)[:20]
                if isinstance(truth, dict) else str(type(truth))}

    rules = [dict(r) for r in db.query("SELECT * FROM rules")]

    # --- what they say they sent -------------------------------------------
    # Two passes: deletes can appear anywhere in the list (that's the point of
    # them), so we have to know the full deleted set before judging any match.
    deliveries = 0
    distinct_ids: set[str] = set()
    created_deliveries = 0
    deleted_comments: set[str] = set()
    matches: list[tuple[str, str, str]] = []   # (rule_id, user_id, comment_id)

    for event in events:
        event_id, event_type, comment_id, user_id, text = _event_fields(event)
        deliveries += 1
        if event_id:
            distinct_ids.add(event_id)
        if event_type == "comment.deleted":
            if comment_id:
                deleted_comments.add(comment_id)
            continue
        created_deliveries += 1
        lowered = text.lower()
        for rule in rules:
            if rule["keyword_lc"] and rule["keyword_lc"] in lowered and user_id:
                matches.append((rule["rule_id"], user_id, comment_id))

    # There is no single correct expected count, and pretending otherwise is
    # how you end up "fixing" correct behaviour. Deletes make the expectation
    # timing-dependent: a comment deleted *before* we got its DM out should
    # produce nothing, while the same comment deleted *after* we sent correctly
    # keeps its DM. The truth file records what was sent, not the interleaving
    # against our own send clock, so the honest answer is a band.
    #
    #   upper bound  every delete lost the race -> every match owed a DM
    #   lower bound  every delete won the race  -> matches on deleted comments
    #                                              owed nothing
    upper_pairs = {(r, u) for r, u, _ in matches}
    upper_duplicates = len(matches) - len(upper_pairs)

    live = [(r, u) for r, u, c in matches if c not in deleted_comments]
    lower_pairs = set(live)
    lower_duplicates = len(live) - len(lower_pairs)

    # --- what we actually did ----------------------------------------------
    ours = stats.verbose_stats()
    our_event_ids = {
        r["event_id"] for r in db.query("SELECT event_id FROM events")
    }
    our_pairs = {
        (r["rule_id"], r["user_id"])
        for r in db.query("SELECT rule_id, user_id FROM dm_tasks")
    }

    ours_dm_count = len(our_pairs)
    ours_duplicates = ours["duplicates_blocked"]

    # Three checks that are exact regardless of delete timing.
    missing_events = sorted(distinct_ids - our_event_ids)
    # A pair with at least one comment that was never deleted is owed a DM,
    # whatever the timing. Not creating it is a real miss.
    missing_pairs = sorted(lower_pairs - our_pairs)
    # A pair we created that matches nothing at all in the truth file is a real
    # false positive -- we invented a DM.
    unjustified_pairs = sorted(our_pairs - upper_pairs)

    in_dm_band = len(lower_pairs) <= ours_dm_count <= len(upper_pairs)
    in_dupe_band = lower_duplicates <= ours_duplicates <= upper_duplicates

    terminal = ours["sent"] + ours["failed"] + ours["detail"]["cancelled_by_delete"]
    return {
        "run_id": run_id,
        "verdict": {
            "events_lost_in_transit": len(missing_events),
            "dms_we_should_have_created_and_didnt": len(missing_pairs),
            "dms_we_created_that_truth_doesnt_justify": len(unjustified_pairs),
            "dm_count_within_expected_band": in_dm_band,
            "duplicates_within_expected_band": in_dupe_band,
            "clean": (not missing_events and not missing_pairs
                      and not unjustified_pairs and in_dm_band and in_dupe_band),
        },
        "truth": {
            "event_deliveries": deliveries,
            "distinct_event_ids": len(distinct_ids),
            "redeliveries": deliveries - len(distinct_ids),
            "comment_created_deliveries": created_deliveries,
            "comment_deleted": len(deleted_comments),
            "rule_matches_total": len(matches),
        },
        # The band, and where we land in it. Sitting strictly inside is the
        # expected result when deletes race sends: some deletes won, some lost.
        "expected_band": {
            "unique_dms": {
                "if_every_delete_won_the_race": len(lower_pairs),
                "ours": ours_dm_count,
                "if_every_delete_lost_the_race": len(upper_pairs),
            },
            "duplicates_blocked": {
                "if_every_delete_won_the_race": lower_duplicates,
                "ours": ours_duplicates,
                "if_every_delete_lost_the_race": upper_duplicates,
            },
            "our_split": {
                "deletes_that_beat_the_send": ours["detail"]["cancelled_by_delete"],
                "deletes_already_applied_at_match_time":
                    ours["detail"]["suppressed_deleted"],
            },
        },
        "ours": {
            "event_deliveries": ours["detail"]["event_deliveries"],
            "distinct_event_ids": ours["detail"]["events_distinct"],
            "dm_tasks": sum(ours["detail"]["tasks_by_state"].values()),
            **{k: ours[k] for k in ("sent", "failed", "queued", "duplicates_blocked")},
            "cancelled_by_delete": ours["detail"]["cancelled_by_delete"],
            "drain_progress": f"{terminal}/{terminal + ours['queued']}",
            "invariants": ours["detail"]["invariants"],
        },
        "samples": {
            "missing_events": missing_events[:10],
            "missing_dms": [{"rule_id": r, "user_id": u} for r, u in missing_pairs[:10]],
            "unjustified_dms": [{"rule_id": r, "user_id": u}
                                for r, u in unjustified_pairs[:10]],
        },
    }
