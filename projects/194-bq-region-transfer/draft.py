# ===============================
# Imports
# ===============================
import subprocess
import json
import os
import sys

# ===============================
# Configurable Parameters
# ===============================
PROJECT = "data-analytics-389803"
SOURCE_DATASET = "op_20250702"
DEST_DATASET = "op_20250702_US"
BUCKET = "conflixis-temp"
REGION_SRC = "us-east4"
REGION_DEST = "US"
PROGRESS_FILE = "migration_progress.json"

# ===============================
# Helpers
# ===============================
def run_cmd(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running: {cmd}\n{result.stderr}")
        sys.exit(1)
    return result.stdout.strip()

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"completed": []}

def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)

# ===============================
# Main Processing Functions
# ===============================
def list_tables():
    cmd = f"bq ls --format=json --project_id={PROJECT} {SOURCE_DATASET}"
    output = run_cmd(cmd)
    tables = [t["tableReference"]["tableId"] for t in json.loads(output)]
    return tables

def export_table(table):
    gcs_path = f"gs://{BUCKET}/{SOURCE_DATASET}/{table}/*.parquet"
    cmd = (
        f"bq extract --location={REGION_SRC} --destination_format=PARQUET "
        f"{PROJECT}:{SOURCE_DATASET}.{table} {gcs_path}"
    )
    run_cmd(cmd)
    return gcs_path

def load_table(table, gcs_path):
    cmd = (
        f"bq load --location={REGION_DEST} --source_format=PARQUET "
        f"{PROJECT}:{DEST_DATASET}.{table} {gcs_path}"
    )
    run_cmd(cmd)

def migrate():
    tables = list_tables()
    progress = load_progress()
    completed = set(progress["completed"])

    print(f"Found {len(tables)} tables. {len(completed)} already done.")

    for i, table in enumerate(tables, 1):
        if table in completed:
            print(f"[{i}/{len(tables)}] Skipping {table} (already migrated)")
            continue

        print(f"[{i}/{len(tables)}] Migrating {table}...")
        gcs_path = export_table(table)
        load_table(table, gcs_path)

        completed.add(table)
        progress["completed"] = list(completed)
        save_progress(progress)

        print(f"✔ {table} done")

    print("✅ Migration completed!")

# ===============================
# Script Execution
# ===============================
if __name__ == "__main__":
    migrate()
