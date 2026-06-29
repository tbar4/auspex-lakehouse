#!/usr/bin/env python3
"""Cancel Dagster backfills that target asset keys which no longer exist.

After the bronze descriptive rename (PR #11) the asset keys changed, e.g.
`neo_lookup` -> `nasa_near_earth_object_lookups`. Any backfill that was still
in-flight at rename time keeps its OLD target keys in the Dagster instance DB
(the `bulk_actions` table). On every iteration the backfill daemon re-loads such
a record, fails to validate its partitions against the current asset graph, and
raises:

    DagsterAssetBackfillDataLoadError: Asset AssetKey(['neo_lookup']) existed at
    storage-time, but no longer does.

That aborts the daemon iteration and blocks ALL backfills. This script finds the
non-terminal backfills whose target asset keys are absent from the live asset
graph and cancels them via the webserver GraphQL API — the same action the UI
"Cancel" button performs, scriptable for when the UI itself can't load them.

It NEVER cancels a backfill whose targets all still exist, and defaults to a
read-only dry run.

Env:
  DAGSTER_GRAPHQL_URL   GraphQL endpoint (default http://localhost:3000/graphql)

Usage:
  python cancel_stale_backfills.py list                # read-only: list backfills + classify
  python cancel_stale_backfills.py cancel-stale        # DRY-RUN: show what would be canceled
  python cancel_stale_backfills.py cancel-stale --yes-really-cancel   # actually cancel them
"""
import os
import sys

import requests

# Backfill statuses that are still live (worth canceling). Terminal states
# (COMPLETED_SUCCESS / COMPLETED_FAILED / CANCELED / FAILED / COMPLETED) are left alone.
ACTIVE_STATUSES = {"REQUESTED", "CANCELING"}

_LIST_BACKFILLS = """
query Backfills {
  partitionBackfillsOrError {
    __typename
    ... on PartitionBackfills {
      results { id status timestamp numPartitions assetSelection { path } }
    }
    ... on PythonError { message }
  }
}
"""

_LIST_ASSETS = """
query Assets {
  assetNodes { assetKey { path } }
}
"""

_CANCEL = """
mutation Cancel($backfillId: String!) {
  cancelPartitionBackfill(backfillId: $backfillId) {
    __typename
    ... on CancelBackfillSuccess { backfillId }
    ... on UnauthorizedError { message }
    ... on PythonError { message }
  }
}
"""


def _endpoint():
    return os.getenv("DAGSTER_GRAPHQL_URL", "http://localhost:3000/graphql")


def _gql(query, variables=None):
    resp = requests.post(
        _endpoint(), json={"query": query, "variables": variables or {}}, timeout=60
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("errors"):
        raise RuntimeError(f"GraphQL errors: {payload['errors']}")
    return payload["data"]


def _live_asset_keys():
    """Set of '/'-joined asset keys currently defined in the code location(s)."""
    data = _gql(_LIST_ASSETS)
    return {"/".join(n["assetKey"]["path"]) for n in data["assetNodes"]}


def _backfills():
    data = _gql(_LIST_BACKFILLS)["partitionBackfillsOrError"]
    if data["__typename"] != "PartitionBackfills":
        raise RuntimeError(f"could not list backfills: {data.get('message')}")
    return data["results"]


def _missing_targets(bf, live):
    """Target asset keys of `bf` that are absent from the live asset graph."""
    return [
        "/".join(a["path"]) for a in (bf.get("assetSelection") or [])
        if "/".join(a["path"]) not in live
    ]


def _classify(bf, live):
    missing = _missing_targets(bf, live)
    if bf["status"] not in ACTIVE_STATUSES:
        return "terminal — skip", missing
    if missing:
        return "STALE -> cancelable", missing
    return "active, targets OK", missing


def cmd_list():
    live = _live_asset_keys()
    backfills = _backfills()
    print(f"{len(backfills)} backfill(s) at {_endpoint()} "
          f"({len(live)} live asset keys):\n")
    for bf in backfills:
        tag, missing = _classify(bf, live)
        print(f"  {bf['id']:24s} {bf['status']:12s} parts={bf['numPartitions']!s:>5}  [{tag}]")
        if missing:
            print(f"      missing target keys: {', '.join(missing)}")


def cmd_cancel_stale(really):
    live = _live_asset_keys()
    stale = [
        bf for bf in _backfills()
        if bf["status"] in ACTIVE_STATUSES and _missing_targets(bf, live)
    ]
    print(f"{'CANCELING' if really else 'DRY-RUN — would cancel'} "
          f"{len(stale)} stale backfill(s):")
    for bf in stale:
        print(f"  - {bf['id']}  (missing: {', '.join(_missing_targets(bf, live))})")
    if not stale:
        print("  (none — no live backfill targets a renamed/removed asset)")
        return
    if not really:
        print("\nRe-run with --yes-really-cancel to actually cancel.")
        return
    for bf in stale:
        result = _gql(_CANCEL, {"backfillId": bf["id"]})["cancelPartitionBackfill"]
        if result["__typename"] == "CancelBackfillSuccess":
            print(f"  canceled {bf['id']}")
        else:
            print(f"  FAILED to cancel {bf['id']}: {result.get('message')}\n"
                  f"    (fall back to updating the bulk_actions row in the Dagster "
                  f"Postgres, then restart the daemon)")


def main(argv):
    if not argv or argv[0] not in {"list", "cancel-stale"}:
        print(__doc__)
        return 2
    if argv[0] == "list":
        cmd_list()
    else:
        cmd_cancel_stale(really="--yes-really-cancel" in argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
