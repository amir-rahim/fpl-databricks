"""
FPL -> Databricks Unity Catalog Volume ingestion.

Runs OUTSIDE Databricks (in GitHub Actions), where outbound internet access
isn't restricted the way Free Edition serverless compute is. It pulls raw
data from the public FPL API and uploads it as JSON into a UC Volume via
the Databricks Files API (through the SDK). Bronze notebooks/jobs inside
Databricks then read from that volume.

Env vars required (set as GitHub Actions secrets):
    DATABRICKS_HOST   e.g. https://<workspace>.cloud.databricks.com
    DATABRICKS_TOKEN  a personal access token with write access to the volume

NOTE: this hits /element-summary/{id}/ once per player (~700 calls per run).
The FPL API doesn't publish a documented rate limit -- if you see 429s in
the logs, add a short sleep between requests (a commented-out line is left
below to make that easy).
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests
from databricks.sdk import WorkspaceClient

FPL_BASE = "https://fantasy.premierleague.com/api"
VOLUME_ROOT = "/Volumes/fpl/bronze/landing"  # matches sql/00_setup_schema.sql

session = requests.Session()
session.headers.update({"User-Agent": "fpl-databricks-ingest/1.0"})


def fetch_json(endpoint: str) -> dict:
    resp = session.get(f"{FPL_BASE}/{endpoint}", timeout=30)
    resp.raise_for_status()
    return resp.json()


def upload_to_volume(client: WorkspaceClient, volume_path: str, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    client.files.upload(volume_path, data, overwrite=True)
    print(f"  uploaded {len(data):,} bytes -> {volume_path}")


def main() -> None:
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    client = WorkspaceClient(host=host, token=token)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"Starting FPL ingestion run {run_ts}")

    # 1. bootstrap-static: players, teams, gameweeks, positions
    print("Fetching bootstrap-static...")
    bootstrap = fetch_json("bootstrap-static/")
    upload_to_volume(
        client,
        f"{VOLUME_ROOT}/bootstrap_static/{run_ts}.json",
        bootstrap,
    )

    current_event = next(
        (e["id"] for e in bootstrap["events"] if e.get("is_current")), None
    )

    # 2. fixtures
    print("Fetching fixtures...")
    fixtures = fetch_json("fixtures/")
    upload_to_volume(
        client,
        f"{VOLUME_ROOT}/fixtures/{run_ts}.json",
        fixtures,
    )

    # 3. per-player gameweek history (element-summary), batched into one file
    player_ids = [el["id"] for el in bootstrap["elements"]]
    print(f"Fetching element-summary for {len(player_ids)} players...")
    all_histories = []
    failures = []
    for i, pid in enumerate(player_ids, start=1):
        try:
            summary = fetch_json(f"element-summary/{pid}/")
            all_histories.append({"player_id": pid, **summary})
        except requests.HTTPError as exc:
            failures.append(pid)
            print(f"  WARN: player {pid} failed: {exc}", file=sys.stderr)
        # time.sleep(0.05)  # uncomment if you start seeing 429s
        if i % 100 == 0:
            print(f"  ...{i}/{len(player_ids)}")

    upload_to_volume(
        client,
        f"{VOLUME_ROOT}/player_gameweek_history/{run_ts}.json",
        {"gameweek": current_event, "players": all_histories},
    )

    if failures:
        print(f"WARNING: {len(failures)} player(s) failed: {failures}", file=sys.stderr)

    print(f"Ingestion run {run_ts} complete.")


if __name__ == "__main__":
    main()
