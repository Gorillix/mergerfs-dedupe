import json
import os
import sys
import logging
import csv
import argparse
import shutil
from pathlib import Path

# --- Set up logging to console and a file ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler("script_run.log"), # General script activity log.
        logging.StreamHandler(sys.stdout) # Also print to the console.
    ]
)

def check_dependencies():
    """Checks if required command-line tools are installed."""
    logging.info("--- Checking for required dependencies... ---")
    required_apps = ['jdupes', 'jq']
    missing_apps = []
    for app in required_apps:
        if not shutil.which(app):
            missing_apps.append(app)
    
    if missing_apps:
        logging.error("ERROR: The following required applications are not installed or not in your PATH:")
        for app in missing_apps:
            logging.error(f"  - {app}")
        logging.error("\nPlease install them to continue. On Debian/Ubuntu, you can use:")
        logging.error(f"  sudo apt-get update && sudo apt-get install {' '.join(missing_apps)}")
        sys.exit(1)
    
    logging.info("All dependencies are satisfied.")


def load_processed_files(log_path):
    """Reads the state log to see which files have already been handled."""
    processed = {}
    if not os.path.exists(log_path):
        # Create the log file with a header if it doesn't exist
        try:
            with open(log_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['filepath', 'status'])
            return processed
        except IOError as e:
            logging.error(f"Could not create state log {log_path}: {e}")
            return None # Indicate failure
    try:
        with open(log_path, 'r', newline='') as f:
            reader = csv.reader(f)
            next(reader, None) # Skip header
            for row in reader:
                if row:
                    processed[row[0]] = row[1]
    except IOError as e:
        logging.error(f"Could not read state log {log_path}: {e}")
    return processed

def update_state_log(log_path, filepath, status):
    """Updates the state log by rewriting it with the new status for a file."""
    # Read all data, update in memory, and write back. This is safer for state changes.
    rows = []
    fieldnames = ['filepath', 'status']
    try:
        with open(log_path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['filepath'] == filepath:
                    row['status'] = status
                rows.append(row)
        
        # Add new file if it wasn't in the log
        if not any(r['filepath'] == filepath for r in rows):
            rows.append({'filepath': filepath, 'status': status})

        with open(log_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    except IOError as e:
        logging.error(f"Could not write to state log {log_path}: {e}")


def get_physical_path(mergerfs_path, pool_root, primary_path):
    """Resolves the underlying physical path from a mergerfs path."""
    try:
        result = os.stat(mergerfs_path)
        for disk in os.listdir(pool_root):
            disk_path = os.path.join(pool_root, disk)
            if os.path.isdir(disk_path) and os.stat(disk_path).st_dev == result.st_dev:
                mergerfs_base = Path(primary_path).parent
                relative_path = Path(mergerfs_path).relative_to(mergerfs_base)
                return os.path.join(disk_path, relative_path)
    except (FileNotFoundError, OSError, ValueError) as e:
        logging.warning(f"Could not resolve physical path for {mergerfs_path}: {e}")
    return None

def link_file(master_physical_path, dup_file, primary_path, log_file):
    """Creates directories and the final hardlink."""
    mergerfs_base = Path(primary_path).parent
    # Correctly determine the physical disk root (e.g., /mnt/pool/disk1)
    physical_disk_root = Path(master_physical_path).parents[len(Path(master_physical_path).relative_to(mergerfs_base).parents) - 1]
    
    relative_dup_dir = Path(dup_file).parent.relative_to(mergerfs_base)
    link_target_dir = os.path.join(physical_disk_root, relative_dup_dir)
    link_target_path = os.path.join(link_target_dir, os.path.basename(dup_file))

    logging.info(f"    - Ensuring directory exists: {link_target_dir}")
    Path(link_target_dir).mkdir(parents=True, exist_ok=True)

    logging.info(f"    - Creating hardlink at: {link_target_path}")
    os.link(master_physical_path, link_target_path)
    update_state_log(log_file, dup_file, 'LINKED')
    logging.info(f"    - SUCCESS! Linked {dup_file}")

def run_deduplication(json_file, log_file, primary_path, pool_root, perform_actions):
    """Main function to execute the deduplication process."""
    if not perform_actions:
        logging.warning("--- DRY RUN MODE: No files will be deleted or linked. ---")

    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
        match_sets = data.get('matchSets', [])
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Could not read or parse manifest {json_file}: {e}")
        sys.exit(1)

    processed_files = load_processed_files(log_file)
    logging.info(f"Loaded {len(processed_files)} entries from the state log.")

    # --- Recovery Step: Handle incomplete tasks first ---
    logging.info("--- Checking for incomplete tasks from previous runs... ---")
    recovery_needed = {k: v for k, v in processed_files.items() if v == 'DELETED'}
    if not recovery_needed:
        logging.info("No incomplete tasks found.")
    else:
        logging.warning(f"Found {len(recovery_needed)} files needing recovery (status was DELETED).")
        # Create a quick lookup map for all files in the manifest
        file_to_set_map = {f['filePath']: s for s in match_sets for f in s['files']}
        
        for dup_file in recovery_needed.keys():
            logging.info(f"Attempting to recover failed link for: {dup_file}")
            if dup_file not in file_to_set_map:
                logging.error(f"  - Could not find {dup_file} in the manifest. Cannot recover.")
                continue
            
            match_set = file_to_set_map[dup_file]
            master_file = next((f['filePath'] for f in match_set['files'] if f.startswith(primary_path)), None)

            if not master_file:
                logging.error(f"  - Could not find a master file for {dup_file}. Cannot recover.")
                continue

            master_physical_path = get_physical_path(master_file, pool_root, primary_path)
            if not master_physical_path:
                logging.error(f"  - FATAL: Could not determine physical path for master file {master_file}. Cannot recover.")
                continue
            
            try:
                if perform_actions:
                    link_file(master_physical_path, dup_file, primary_path, log_file)
                else:
                    logging.info(f"  - [Dry Run] Would recover link for {dup_file}")
            except Exception as e:
                logging.error(f"  - FAILED to recover link for {dup_file}. Error: {e}")

    # --- Main Processing Loop ---
    logging.info(f"--- Starting to process {len(match_sets)} duplicate sets... ---")
    for match_set in match_sets:
        files = [f['filePath'] for f in match_set['files']]

        master_file = next((f for f in files if f.startswith(primary_path)), None)
        if not master_file:
            logging.warning(f"Skipping set, no master file found in {primary_path}. Files: {files}")
            continue

        logging.info(f"Processing set. Master file is: {master_file}")

        for dup_file in files:
            if dup_file == master_file:
                continue

            if processed_files.get(dup_file) == 'LINKED':
                logging.info(f"Skipping already linked file: {dup_file}")
                continue

            logging.info(f"  - Found duplicate: {dup_file}")

            master_physical_path = get_physical_path(master_file, pool_root, primary_path)
            if not master_physical_path:
                logging.error(f"    - FATAL: Could not determine physical path for master file {master_file}. Skipping.")
                continue
            
            if perform_actions:
                try:
                    update_state_log(log_file, dup_file, 'PENDING')
                    
                    logging.info(f"    - Deleting: {dup_file}")
                    os.remove(dup_file)
                    update_state_log(log_file, dup_file, 'DELETED')

                    link_file(master_physical_path, dup_file, primary_path, log_file)
                except Exception as e:
                    logging.error(f"    - FAILED to process {dup_file}. Error: {e}")
            else:
                logging.info(f"    - Would delete: {dup_file}")
                logging.info(f"    - Would create hardlink from '{master_physical_path}'")

    logging.info("--- Deduplication script finished. ---")


if __name__ == "__main__":
    # --- Pre-flight Checks ---
    check_dependencies()

    parser = argparse.ArgumentParser(
        description="Finds and consolidates duplicate files across a mergerfs pool by hardlinking.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--json-file',
        required=True,
        help="Path to the duplicates.json manifest file generated by jdupes."
    )
    parser.add_argument(
        '--log-file',
        required=True,
        help="Path to the CSV log file for tracking operation states."
    )
    parser.add_argument(
        '--primary-path',
        required=True,
        help="The primary path. Files within this path are considered 'masters' and will be preserved."
    )
    parser.add_argument(
        '--pool-root',
        required=True,
        help="The root directory containing the individual disk mounts (e.g., /mnt/pool/)."
    )
    parser.add_argument(
        '--perform-actions',
        action='store_true',
        help="Actually perform delete and link operations. Without this flag, the script runs in dry-run mode."
    )

    args = parser.parse_args()

    if not os.path.exists(args.json_file):
        logging.error(f"Manifest file not found at {args.json_file}. Please run the jdupes scan first.")
        sys.exit(1)
    
    run_deduplication(
        json_file=args.json_file,
        log_file=args.log_file,
        primary_path=args.primary_path,
        pool_root=args.pool_root,
        perform_actions=args.perform_actions
    )
