import json
import os
import sys
import logging
import sqlite3
import argparse
import shutil
from pathlib import Path

def setup_logging():
    """Configures a robust logger."""
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()
    
    logger.setLevel(logging.INFO)
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')

    file_handler = logging.FileHandler("script_run.log")
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

def check_dependencies():
    """Checks if required command-line tools are installed."""
    logging.info("--- Checking for required dependencies... ---")
    # jdupes is the only hard requirement for the script's core logic.
    required_apps = ['jdupes']
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

def db_initialize(db_path):
    """Creates and initializes the SQLite database if it doesn't exist."""
    try:
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS processed_files (
                filepath TEXT PRIMARY KEY,
                status TEXT NOT NULL
            )
        ''')
        con.commit()
        con.close()
        return True
    except sqlite3.Error as e:
        logging.error(f"Database error during initialization: {e}")
        return False

def load_processed_files(db_path):
    """Reads the state DB to see which files have already been handled."""
    processed = {}
    try:
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        for row in cur.execute("SELECT filepath, status FROM processed_files"):
            processed[row[0]] = row[1]
        con.close()
    except sqlite3.Error as e:
        logging.error(f"Could not read state from database {db_path}: {e}")
    return processed

def update_state_db(db_connection, filepath, status):
    """Atomically inserts or replaces a file's status using an open DB connection."""
    try:
        cur = db_connection.cursor()
        cur.execute("INSERT OR REPLACE INTO processed_files (filepath, status) VALUES (?, ?)", (filepath, status))
        db_connection.commit()
    except sqlite3.Error as e:
        logging.error(f"Could not write to state database: {e}")

def get_physical_path(mergerfs_path, pool_root):
    """Resolves the underlying physical path and its disk root from a mergerfs path."""
    try:
        physical_path = os.path.realpath(mergerfs_path)
        # Check if the real path is within one of the pool disks
        for disk in os.listdir(pool_root):
            disk_path = os.path.join(pool_root, disk)
            if os.path.isdir(disk_path) and physical_path.startswith(disk_path):
                return physical_path, disk_path
    except (FileNotFoundError, OSError) as e:
        logging.warning(f"Could not resolve physical path for {mergerfs_path}: {e}")
    return None, None

def link_file(db_connection, master_physical_path, physical_disk_root, dup_file, primary_path):
    """Creates directories and the final hardlink."""
    mergerfs_base = Path(primary_path).parent
    relative_dup_dir = Path(dup_file).parent.relative_to(mergerfs_base)
    link_target_dir = os.path.join(physical_disk_root, relative_dup_dir)
    link_target_path = os.path.join(link_target_dir, os.path.basename(dup_file))

    logging.info(f"    - Ensuring directory exists: {link_target_dir}")
    Path(link_target_dir).mkdir(parents=True, exist_ok=True)

    logging.info(f"    - Creating hardlink at: {link_target_path}")
    os.link(master_physical_path, link_target_path)
    update_state_db(db_connection, dup_file, 'LINKED')
    logging.info(f"    - SUCCESS! Linked {dup_file}")

def run_deduplication(args):
    """Main function to execute the deduplication process."""
    if not args.perform_actions:
        logging.warning("--- DRY RUN MODE: No files will be deleted or linked. ---")

    try:
        with open(args.json_file, 'r') as f:
            data = json.load(f)
        match_sets = data.get('matchSets', [])
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Could not read or parse manifest {args.json_file}: {e}")
        sys.exit(1)

    processed_files = load_processed_files(args.db_file)
    file_to_set_map = {f['filePath']: s for s in match_sets for f in s['files']}
    
    logging.info(f"Loaded {len(processed_files)} entries from the state database.")

    db_connection = sqlite3.connect(args.db_file)
    try:
        # --- Recovery Step ---
        logging.info("--- Checking for incomplete tasks from previous runs... ---")
        recovery_needed = {k: v for k, v in processed_files.items() if v in ['DELETED', 'PENDING']}
        if not recovery_needed:
            logging.info("No incomplete tasks found.")
        else:
            logging.warning(f"Found {len(recovery_needed)} files needing recovery.")
            for dup_file in recovery_needed.keys():
                logging.info(f"Attempting to recover failed link for: {dup_file}")
                if dup_file not in file_to_set_map:
                    logging.error(f"  - Could not find {dup_file} in the manifest. Cannot recover.")
                    continue
                
                match_set = file_to_set_map[dup_file]
                master_file = next((f['filePath'] for f in match_set['files'] if f.startswith(args.primary_path)), None)

                if not master_file:
                    logging.error(f"  - Could not find a master file for {dup_file}. Cannot recover.")
                    continue

                master_physical_path, physical_disk_root = get_physical_path(master_file, args.pool_root)
                if not master_physical_path:
                    logging.error(f"  - FATAL: Could not determine physical path for master {master_file}. Cannot recover.")
                    continue
                
                try:
                    if args.perform_actions:
                        link_file(db_connection, master_physical_path, physical_disk_root, dup_file, args.primary_path)
                    else:
                        logging.info(f"  - [Dry Run] Would recover link for {dup_file}")
                except Exception as e:
                    logging.error(f"  - FAILED to recover link for {dup_file}. Error: {e}")

        # --- REFRESH the dictionary after potential recovery actions ---
        logging.info("Refreshing file status after recovery check...")
        processed_files = load_processed_files(args.db_file)

        # --- Main Processing Loop ---
        logging.info(f"--- Starting to process {len(match_sets)} duplicate sets... ---")
        for match_set in match_sets:
            files = [f['filePath'] for f in match_set['files']]
            master_file = next((f for f in files if f.startswith(args.primary_path)), None)
            
            if not master_file:
                continue

            logging.info(f"Processing set for master: {master_file}")
            
            master_physical_path, physical_disk_root = get_physical_path(master_file, args.pool_root)
            if not master_physical_path:
                logging.error(f"    - FATAL: Could not determine physical path for master {master_file}. Skipping set.")
                continue

            for dup_file in files:
                # This is the correct, per-file check. It is fast and reliable.
                if dup_file == master_file or processed_files.get(dup_file) == 'LINKED':
                    continue

                logging.info(f"  - Found duplicate: {dup_file}")
                
                if args.perform_actions:
                    try:
                        update_state_db(db_connection, dup_file, 'PENDING')
                        
                        logging.info(f"    - Deleting: {dup_file}")
                        os.remove(dup_file)
                        update_state_db(db_connection, dup_file, 'DELETED')

                        link_file(db_connection, master_physical_path, physical_disk_root, dup_file, args.primary_path)
                    except FileNotFoundError:
                         logging.warning(f"    - File not found for deletion (already gone?): {dup_file}. Attempting to link.")
                         try:
                             link_file(db_connection, master_physical_path, physical_disk_root, dup_file, args.primary_path)
                         except Exception as e:
                             logging.error(f"    - FAILED to process {dup_file} after FileNotFoundError. Error: {e}")
                    except Exception as e:
                        logging.error(f"    - FAILED to process {dup_file}. Error: {e}")
                else:
                    logging.info(f"    - Would delete: {dup_file}")
                    logging.info(f"    - Would create hardlink from '{master_physical_path}'")
    finally:
        if 'db_connection' in locals() and db_connection:
            db_connection.close()
            logging.info("--- Database connection closed. ---")

    logging.info("--- Deduplication script finished. ---")

if __name__ == "__main__":
    setup_logging()
    check_dependencies()

    parser = argparse.ArgumentParser(
        description="Finds and consolidates duplicate files across a mergerfs pool by hardlinking.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--json-file', required=True, help="Path to the duplicates.json manifest.")
    parser.add_argument('--db-file', required=True, help="Path to the SQLite database for tracking state.")
    parser.add_argument('--primary-path', required=True, help="The primary path where 'master' files are kept.")
    parser.add_argument('--pool-root', required=True, help="The root directory of individual disk mounts (e.g., /mnt/pool/).")
    parser.add_argument('--perform-actions', action='store_true', help="Perform delete/link operations. Default is dry-run.")
    
    args = parser.parse_args()

    if not os.path.exists(args.json_file):
        logging.error(f"Manifest file not found: {args.json_file}")
        sys.exit(1)
    if not os.path.isdir(args.primary_path):
        logging.error(f"Primary path not found or not a directory: {args.primary_path}")
        sys.exit(1)
    if not os.path.isdir(args.pool_root):
        logging.error(f"Pool root not found or not a directory: {args.pool_root}")
        sys.exit(1)
    if not db_initialize(args.db_file):
        sys.exit(1)
    
    run_deduplication(args)
