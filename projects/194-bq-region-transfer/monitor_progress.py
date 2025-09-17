#!/usr/bin/env python3
"""
Monitor BigQuery transfer progress
"""

import json
import time
from pathlib import Path
from datetime import datetime

def monitor_progress():
    """Monitor transfer progress"""
    progress_file = Path("migration_progress.json")

    print("=" * 60)
    print("BigQuery Transfer Progress Monitor")
    print("=" * 60)

    while True:
        try:
            if progress_file.exists():
                with open(progress_file, 'r') as f:
                    data = json.load(f)

                completed = data.get('completed', [])
                failed = data.get('failed', [])
                last_updated = data.get('last_updated', 'Unknown')

                # Clear screen (optional)
                print("\033[H\033[J")

                print("=" * 60)
                print(f"Transfer Progress - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 60)
                print(f"✅ Completed: {len(completed)} tables")
                print(f"❌ Failed: {len(failed)} tables")
                print(f"📊 Total: 35 tables")
                print(f"⏱️  Last updated: {last_updated}")
                print()

                # Calculate percentage
                percentage = (len(completed) / 35) * 100

                # Progress bar
                bar_length = 40
                filled = int(bar_length * len(completed) / 35)
                bar = '█' * filled + '░' * (bar_length - filled)
                print(f"Progress: [{bar}] {percentage:.1f}%")
                print()

                if completed:
                    print("Recent completions:")
                    for table in completed[-5:]:
                        print(f"  ✓ {table}")

                if failed:
                    print("\n⚠️  Failed tables:")
                    for table in failed:
                        print(f"  ✗ {table}")

                # Check if complete
                if len(completed) + len(failed) >= 35:
                    print("\n🎉 Transfer complete!")
                    if failed:
                        print(f"⚠️  {len(failed)} tables failed and may need manual intervention")
                    break

            else:
                print("Waiting for transfer to start...")

            # Check log file for current status
            log_file = Path("transfer.log")
            if log_file.exists():
                # Get last few lines of log
                with open(log_file, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        print("\nLatest activity:")
                        for line in lines[-3:]:
                            if "Migrating" in line or "Successfully" in line or "Failed" in line:
                                print(f"  {line.strip()}")

            time.sleep(5)  # Refresh every 5 seconds

        except KeyboardInterrupt:
            print("\nMonitoring stopped")
            break
        except Exception as e:
            print(f"Error reading progress: {e}")
            time.sleep(5)

if __name__ == "__main__":
    monitor_progress()