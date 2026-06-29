#!/usr/bin/env python3
"""Task 5 — bronze descriptive-rename data migration (re-ingest model).

This does the two things the Dagster re-ingest can't do by itself:

  1. COPY the APOD image blobs (plain objects, not a Delta table, so not
     re-created by a dlt run) from the old prefix to the new one.
  2. (optional, guarded) DELETE the orphaned OLD Delta table folders after you
     have confirmed the new-named tables are populated by a re-ingest run.

It does NOT touch the shared dlt metadata (`bronze/_dlt_loads`,
`bronze/_dlt_pipeline_state`, `bronze/_dlt_version`) and never deletes a NEW
table. The Delta tables themselves are repopulated by re-running the renamed
Dagster assets (merge-on-primary-key is idempotent) — this script only handles
the image blobs + old-folder cleanup.

Env (same vars the apod_images asset / resources/delta.py already use):
  MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BRONZE_BUCKET_NAME

Usage:
  python migrate_bronze_rename.py list          # read-only: show bronze/ folders + classify
  python migrate_bronze_rename.py copy-images    # copy apod_images -> new prefix (idempotent)
  python migrate_bronze_rename.py delete-old     # DRY-RUN list of old folders that would be deleted
  python migrate_bronze_rename.py delete-old --yes-really-delete   # actually delete old folders
"""
import os
import sys

import boto3

# old physical table folder -> (kept only to know what is now orphaned).
# The 20 old Delta table names + the apod_images blob prefix.
OLD_TABLE_NAMES = [
    # NASA api
    "apod", "neows", "neo_lookup",
    # DONKI
    "cme", "cme_analysis", "gst", "ips", "flr", "sep", "mpc", "rbe", "hss",
    "wsa_enlil_simulations", "notifications",
    # Space-Track
    "gp", "satcat", "boxscore", "decay", "cdm", "tip",
]

# New names (so `list` can confirm they exist after a re-ingest and so cleanup
# never targets them). Mirrors the rename mapping exactly.
NEW_TABLE_NAMES = [
    "nasa_astronomy_picture_of_the_day", "nasa_near_earth_object_feed",
    "nasa_near_earth_object_lookups",
    "nasa_donki_coronal_mass_ejections", "nasa_donki_coronal_mass_ejection_analyses",
    "nasa_donki_geomagnetic_storms", "nasa_donki_interplanetary_shocks",
    "nasa_donki_solar_flares", "nasa_donki_solar_energetic_particles",
    "nasa_donki_magnetopause_crossings", "nasa_donki_radiation_belt_enhancements",
    "nasa_donki_high_speed_streams", "nasa_donki_wsa_enlil_simulations",
    "nasa_donki_notifications",
    "space_track_general_perturbations", "space_track_satellite_catalog",
    "space_track_boxscore", "space_track_decays",
    "space_track_conjunction_data_messages",
    "space_track_tracking_and_impact_predictions",
]

OLD_IMAGE_PREFIX = "bronze/apod_images/"
NEW_IMAGE_PREFIX = "bronze/nasa_astronomy_picture_of_the_day_images/"

# Shared dlt metadata that must NEVER be touched.
PROTECTED_SEGMENTS = {"_dlt_loads", "_dlt_pipeline_state", "_dlt_version"}


def _client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


def _bucket():
    return os.environ["BRONZE_BUCKET_NAME"]


def _iter_keys(s3, bucket, prefix):
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            yield obj["Key"]
        if not resp.get("IsTruncated"):
            return
        token = resp["NextContinuationToken"]


def _segment(key):
    """First path segment under bronze/ — i.e. the table folder (or 'N__child')."""
    rest = key[len("bronze/"):]
    return rest.split("/", 1)[0]


def _is_orphaned_old(segment):
    """True iff `segment` is an OLD table folder (or its dlt child table) and not
    a new name / protected metadata. Exact boundary: segment == N or
    segment startswith N + '__' (so 'cme' never matches 'cme_analysis')."""
    if segment in PROTECTED_SEGMENTS or segment in NEW_TABLE_NAMES:
        return False
    if segment == "apod_images" or segment.startswith("apod_images__"):
        return True
    for n in OLD_TABLE_NAMES:
        if segment == n or segment.startswith(n + "__"):
            return True
    return False


def cmd_list(s3, bucket):
    segments = {}
    for key in _iter_keys(s3, bucket, "bronze/"):
        seg = _segment(key)
        segments[seg] = segments.get(seg, 0) + 1
    print(f"bronze/ folders in {bucket} (segment: object-count):\n")
    for seg in sorted(segments):
        if seg in PROTECTED_SEGMENTS:
            tag = "PROTECTED (dlt meta)"
        elif seg in NEW_TABLE_NAMES:
            tag = "NEW"
        elif _is_orphaned_old(seg):
            tag = "OLD/orphaned -> deletable"
        else:
            tag = "?"
        print(f"  {seg:50s} {segments[seg]:>7d}  [{tag}]")


def cmd_copy_images(s3, bucket):
    n = 0
    for key in _iter_keys(s3, bucket, OLD_IMAGE_PREFIX):
        dest = NEW_IMAGE_PREFIX + key[len(OLD_IMAGE_PREFIX):]
        s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=dest)
        n += 1
        print(f"  copied {key} -> {dest}")
    print(f"\ncopied {n} image object(s).")
    if n == 0:
        print("  (nothing under bronze/apod_images/ — already migrated or empty)")


def cmd_delete_old(s3, bucket, really):
    targets = []
    for key in _iter_keys(s3, bucket, "bronze/"):
        if _is_orphaned_old(_segment(key)):
            targets.append(key)
    folders = sorted({_segment(k) for k in targets})
    print(f"{'DELETING' if really else 'DRY-RUN — would delete'} "
          f"{len(targets)} object(s) across {len(folders)} old folder(s):")
    for f in folders:
        print(f"  - bronze/{f}/")
    if not really:
        print("\nRe-run with --yes-really-delete to actually delete. "
              "Make sure the NEW tables are populated first (run `list`).")
        return
    for i in range(0, len(targets), 1000):
        batch = [{"Key": k} for k in targets[i:i + 1000]]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
    print(f"\ndeleted {len(targets)} object(s).")


def main(argv):
    if not argv or argv[0] not in {"list", "copy-images", "delete-old"}:
        print(__doc__)
        return 2
    s3, bucket = _client(), _bucket()
    cmd = argv[0]
    if cmd == "list":
        cmd_list(s3, bucket)
    elif cmd == "copy-images":
        cmd_copy_images(s3, bucket)
    elif cmd == "delete-old":
        cmd_delete_old(s3, bucket, really="--yes-really-delete" in argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
