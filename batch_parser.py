"""
batch_parser.py
-----------------

This module provides a simple batch processing utility for log files.
It continuously monitors one or more directories for new text files
and parses them into the shared SQLite database using the
``log_parser`` module. This allows long running ingestion of large
log files without blocking the user interface.

Usage:
  python batch_parser.py --dirs /path/to/logs1 /path/to/logs2 --db /path/to/parsed_logs.db

Options:
  --dirs : One or more directory paths to watch for new files. When
           new text files appear in these directories they will be
           processed and inserted into the database. Only files with
           extensions '.txt', '.log' or no extension are processed.
  --db   : Path to the SQLite database file to use.
  --interval : Number of seconds between directory scans. Default is
               60 seconds.
  --quality  : Quality ranking to assign to all uploads processed by
               this watcher. Defaults to 'regular'.

The watcher runs indefinitely until interrupted. It uses the
``log_parser.create_upload`` and ``log_parser.process_file`` functions
to insert data into the database. The database schema must have
already been initialised (e.g. via the Streamlit UI or by calling
``log_parser.init_database`` once before running the watcher).

Note:
  This script should be run as a separate process from the Streamlit UI.
  Both processes can safely access the same SQLite database because
  ``log_parser.init_database`` enables write-ahead logging (WAL) and
  sets appropriate busy timeouts. However, long-running ingestion may
  still impact read performance slightly. To further reduce contention
  you can adjust the scan interval or run the watcher during off-peak
  hours.
"""

import argparse
import os
import time
import sqlite3
from typing import Iterable

import log_parser


def get_processed_filenames(db_path: str) -> set[str]:
    """Return a set of filenames that have already been processed.

    The uploads table stores the original file name for each upload. We
    use this to avoid reprocessing the same file multiple times.

    Parameters
    ----------
    db_path : str
        Path to the SQLite database.

    Returns
    -------
    set[str]
        A set of file names that have been inserted via the uploads
        table.
    """
    processed = set()
    try:
        uploads = log_parser.list_uploads(db_path)
        for (_, _, fname, _) in uploads:
            processed.add(fname)
    except Exception:
        # If the uploads table does not exist yet, assume nothing is processed.
        processed = set()
    return processed


def discover_new_files(directories: Iterable[str], processed: set[str]) -> list[str]:
    """Discover new files in the provided directories.

    Parameters
    ----------
    directories : Iterable[str]
        Paths to directories to scan.
    processed : set[str]
        Set of file names that have already been processed. This set
        will be updated with newly discovered files.

    Returns
    -------
    list[str]
        A list of absolute file paths to new files that should be
        processed.
    """
    new_files = []
    for directory in directories:
        try:
            for entry in os.scandir(directory):
                if not entry.is_file():
                    continue
                # Only process text-like files
                lower_name = entry.name.lower()
                if not (lower_name.endswith(".txt") or lower_name.endswith(".log") or "." not in entry.name):
                    continue
                # Skip files already processed
                if entry.name in processed:
                    continue
                new_files.append(entry.path)
        except FileNotFoundError:
            continue
    return new_files


def process_new_files(files: list[str], db_path: str, quality: str) -> None:
    """Process a list of new files into the database.

    For each file, a new upload record is created with the given
    quality ranking. Then the file is parsed and inserted into the
    database using ``log_parser.process_file_with_progress``. The
    upload ID is passed through to associate each record with its
    upload. Using ``process_file_with_progress`` avoids re‑initialising
    the database schema on every file and thus reduces the chance of
    encountering locked database errors.

    Parameters
    ----------
    files : list[str]
        A list of absolute file paths to process.
    db_path : str
        Path to the SQLite database.
    quality : str
        Quality ranking to assign to each upload.
    """
    for file_path in files:
        file_name = os.path.basename(file_path)
        # Create a new upload record. This returns the ID for associating
        # all subsequent log entries with this upload. If the database is
        # temporarily locked (e.g. another process is writing), retry
        # creating the upload a few times with a small delay between attempts.
        upload_id = None
        for attempt in range(5):  # retry up to 5 times
            try:
                upload_id = log_parser.create_upload(db_path, file_name, quality)
                break
            except sqlite3.OperationalError as exc:
                # If the database is locked, wait briefly and try again
                if 'database is locked' in str(exc):
                    time.sleep(1)
                    continue
                # For any other operational error, re-raise
                raise
        if upload_id is None:
            print(f"Failed to create upload for {file_path}: database is locked")
            continue
        try:
            # Process the file without a progress callback. This call will
            # stream the contents into the database but will not re-run
            # log_parser.init_database() (which can cause lock contention).
            log_parser.process_file_with_progress(
                file_path,
                db_path,
                progress_callback=None,
                upload_id=upload_id,
            )
            print(f"Processed {file_path}")
        except Exception as exc:
            # If any error occurs during processing, log it and continue.
            print(f"Failed to process {file_path}: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Continuously watch directories and process new log files.")
    parser.add_argument(
        "--dirs",
        nargs="+",
        required=True,
        help="One or more directories to watch for new log files",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to the SQLite database file",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Number of seconds between scans (default: 60)",
    )
    parser.add_argument(
        "--quality",
        type=str,
        default="regular",
        help="Quality ranking to assign to each upload (default: 'regular')",
    )
    args = parser.parse_args()

    # Ensure the database schema is initialised only when the database file does
    # not already exist. Repeatedly calling init_database while another
    # connection is open can cause lock contention. If the file exists, we
    # assume it has been initialised (e.g. via the Streamlit UI).
    if not os.path.exists(args.db):
        log_parser.init_database(args.db)

    # Track processed files using the uploads table
    processed = get_processed_filenames(args.db)

    print(f"Watching directories: {args.dirs}")
    print(f"Database: {args.db}")
    print(f"Scan interval: {args.interval} seconds")
    print(f"Quality ranking: {args.quality}")

    try:
        while True:
            # Discover new files
            new_files = discover_new_files(args.dirs, processed)
            if new_files:
                # Mark these files as processed to avoid reprocessing
                for nf in new_files:
                    processed.add(os.path.basename(nf))
                # Process files
                process_new_files(new_files, args.db, args.quality)
            # Sleep before next scan
            time.sleep(max(args.interval, 1))
    except KeyboardInterrupt:
        print("Stopping batch parser.")


if __name__ == "__main__":
    main()