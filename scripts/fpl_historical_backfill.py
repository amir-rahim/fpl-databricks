"""
Historical FPL season backfill -> Databricks Unity Catalog Volume.

Pulls players_raw.csv, gws/merged_gw.csv, and fixtures.csv for each season
in SEASONS from the vaastav/Fantasy-Premier-League public GitHub dataset
(static CSVs, no auth, no rate limit) and lands them as raw text in the
same landing volume the live ingestion script writes to.

This is a one-off / occasional job, not part of the recurring ingestion --
run it manually, or on the monthly schedule in the matching workflow file
(the source repo itself is only updated ~3x per season, so there's no
value polling it every 6 hours).

CAVEAT (from the source repo's own README): the `xP` column in
merged_gw.csv is scraped AFTER each gameweek finishes and can leak
post-match information -- their own analysis found unshifted `xP` causes
lookahead bias when used to predict same-gameweek total_points. Exclude
it or shift(1) it per player when you build the Silver layer.

Env vars required (same as fpl_ingest.py):
    DATABRICKS_HOST
    DATABRICKS_TOKEN
"""

import io
import os
from datetime import datetime, timezone

import requests
from databricks.sdk import WorkspaceClient

RAW_BASE = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
VOLUME_ROOT = "/Volumes/fpl/bronze/landing/historical"

# Completed seasons to backfill. Extend this list as more seasons finish --
# deliberately NOT including the current in-progress season here, since
# that's what the live fpl_ingest.py job is already keeping fresh.
SEASONS = ["2021-22", "2022-23", "2023-24", "2024-25"]

FILES = {
    "players_raw": "players_raw.csv",
    "fixtures": "fixtures.csv",
    "merged_gw": "gws/merged_gw.csv",
}

session = requests.Session()
session.headers.update({"User-Agent": "fpl-databricks-backfill/1.0"})


def fetch_csv_text(season: str, relative_path: str) -> str | None:
    url = f"{RAW_BASE}/{season}/{relative_path}"
    resp = session.get(url, timeout=30)
    if resp.status_code == 404:
        print(f"  not found (skipping): {url}")
        return None
    resp.raise_for_status()
    return resp.text


def upload_to_volume(client: WorkspaceClient, volume_path: str, text: str) -> None:
    data = text.encode("utf-8")
    client.files.upload(volume_path, io.BytesIO(data), overwrite=True)
    print(f"  uploaded {len(data):,} bytes -> {volume_path}")


def main() -> None:
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    client = WorkspaceClient(host=host, token=token)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"Starting historical backfill run {run_ts} for seasons: {SEASONS}")

    for season in SEASONS:
        print(f"\nSeason {season}:")
        for file_type, relative_path in FILES.items():
            text = fetch_csv_text(season, relative_path)
            if text is None:
                continue
            volume_path = f"{VOLUME_ROOT}/{season}/{file_type}_{run_ts}.csv"
            upload_to_volume(client, volume_path, text)

    print(f"\nBackfill run {run_ts} complete.")


if __name__ == "__main__":
    main()