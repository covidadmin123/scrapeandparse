"""
log_parser.py
----------------

This module contains a set of functions used to normalise and parse log
files containing credential data. Each line of the input file is expected
to hold information in a colon‐separated format. The parser cleans and
normalises the data, extracts relevant fields and writes them into an
SQLite database for efficient querying.

The high level rules implemented here mirror the requirements provided by
the user:

* Before parsing, each line is cleaned by removing protocol prefixes
  (``http:`` or ``https:``), all whitespace (tabs and spaces) and by
  replacing semicolons, commas or vertical bars with a colon. Any line
  containing ``localhost`` is discarded entirely.
* The remaining line is split on colons. If the second field contains an
  ``@`` symbol it is treated as an e‑mail address, otherwise it is
  considered a username. The third field is treated as the password.
* A new record is inserted into the database with the following fields:
  URL, email (may be ``None``), username (may be ``None``), password
  and a timestamp recording when the record was created.
* Very large files can be processed by streaming the contents in chunks.

These functions are designed to be used both as a stand‑alone script and
as utilities in a web application.
"""

from __future__ import annotations

import os
import re
import sqlite3
from datetime import datetime
from typing import Iterable, Optional, Tuple

# A set of known second-level TLDs for base domain extraction. This is
# intentionally non-exhaustive but covers common cases. When extracting
# the base domain we will consider these TLDs as part of a two-part
# suffix (e.g. 'example.co.uk' → base domain 'example.co.uk').
COMMON_SLD_TLDS = {
    'co.uk', 'org.uk', 'ac.uk', 'gov.uk', 'co.nz', 'com.au', 'net.au', 'org.au',
    'com.br', 'com.cn', 'co.jp', 'co.kr', 'com.mx', 'com.sg', 'com.tr', 'com.sa'
}

CHUNK_SIZE_BYTES = 100 * 1024 * 1024  # 100 MB

def extract_base_domain(url: str) -> str:
    """Extract the base domain from a URL or host string.

    Given a URL without protocol (e.g. 'sub.example.com/path'), this function
    attempts to return the registrable base domain (e.g. 'example.com'). It
    removes any path information and subdomains. If the domain ends with a
    recognised second-level TLD (like 'co.uk'), the base domain will include
    the second-level TLD (e.g. 'example.co.uk'). Otherwise the base domain
    consists of the last two labels (e.g. 'example.com').

    Parameters
    ----------
    url: str
        A cleaned URL string without protocol and possibly containing a path.

    Returns
    -------
    str
        The extracted base domain in lower case.
    """
    if not url:
        return ''
    # Split off any path component
    host = url.split('/')[0]
    host = host.lower()
    # Ignore leading dots
    host = host.lstrip('.')
    parts = host.split('.')
    if len(parts) < 2:
        return host
    tld = parts[-1]
    sld = parts[-2]
    sld_tld = f"{sld}.{tld}"
    # If the suffix is one of our known second-level domains and there is
    # another label available, include three labels.
    if sld_tld in COMMON_SLD_TLDS and len(parts) >= 3:
        return f"{parts[-3]}.{sld_tld}"
    return f"{sld}.{tld}"


def clean_line(line: str) -> Optional[str]:
    """Normalise a raw line of log data.

    This function performs several cleaning steps:

    - Removes leading ``http:`` or ``https:`` prefixes (case insensitive).
    - Strips tabs and spaces from the line while preserving newlines.
    - Replaces semicolons (;), commas (,) and pipes (|) with a colon (:).
    - Discards the line entirely if it contains the substring ``localhost``
      (case insensitive).
    - Strips leading/trailing whitespace characters and returns the cleaned
      line. Returns ``None`` if the line should be skipped.

    Parameters
    ----------
    line: str
        A raw line from the input log file.

    Returns
    -------
    Optional[str]
        A cleaned line ready for parsing or ``None`` if the line
        contains ``localhost``.
    """
    if not line:
        return None
    # Remove http or https protocols including the '://' portion. Use
    # regex to catch mixed case variants and remove the protocol prefix
    # completely (e.g. 'https://example.com' → 'example.com').
    line = re.sub(r"(?i)^https?://", "", line)
    # Remove a leading 'www.' if present to normalise domains (e.g.
    # 'www.example.com' → 'example.com'). This is done after stripping
    # the protocol.
    line = re.sub(r"(?i)^www\.", "", line)
    # Remove tab and space characters but not the newline at the end.
    line = line.replace("\t", "").replace(" ", "")
    # Replace separators ; , | with colon.
    line = line.replace(";", ":").replace(",", ":").replace("|", ":")
    # Skip any line referencing localhost (case insensitive)
    if "localhost" in line.lower():
        return None
    line = line.strip()
    return line or None


def parse_line(cleaned: str) -> Optional[Tuple[str, Optional[str], Optional[str], Optional[str]]]:
    """Parse a cleaned log line into its constituent fields.

    Parameters
    ----------
    cleaned: str
        A line that has been cleaned by :func:`clean_line`.

    Returns
    -------
    Optional[Tuple[str, Optional[str], Optional[str], Optional[str]]]
        A tuple of (url, email, username, password). Email or username
        may be ``None`` depending on the input format. Returns ``None``
        if the line does not contain at least three colon‐separated parts.
    """
    # Split into at most three parts from the right: URL, user/email and password.
    # Using rsplit ensures that colons within the URL (e.g. ``android://``)
    # remain part of the first element rather than being split incorrectly.
    parts = cleaned.rsplit(":", 2)
    if len(parts) < 3:
        return None
    url, second, third = parts
    email: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    # Determine if second part is an email or username
    if "@" in second:
        email = second
    else:
        username = second
    password = third
    return url or None, email, username, password


def init_database(db_path: str) -> None:
    """Initialise the SQLite database with the required table and indexes.

    Parameters
    ----------
    db_path: str
        Path to the SQLite database file. The file will be created if it
        does not exist.
    """
    # Use a longer timeout here to allow the database to become available if
    # another connection is writing. Without this, `sqlite3.connect` will
    # raise an OperationalError ('database is locked') if the database is busy
    # for longer than the default 5 seconds.
    conn = sqlite3.connect(db_path, timeout=30)
    # Attempt to enable write-ahead logging to allow concurrent reads and writes.
    # On some platforms (e.g. Windows) or if the database is already in use, changing
    # the journal mode may raise an OperationalError. To avoid crashes we query
    # the current mode and set it only when needed, swallowing errors.
    try:
        current_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
        if current_mode.lower() != "wal":
            conn.execute("PRAGMA journal_mode=WAL;")
    except sqlite3.OperationalError:
        # Ignore failures; database might be locked or busy and WAL may already be set.
        pass
    # Configure reasonable defaults for durability and busy timeout.
    try:
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
    except sqlite3.OperationalError:
        # Ignore if unable to set due to lock.
        pass

    # Create primary logs table if not exists. We include base_domain and account
    # columns in the initial schema so new databases start with the correct
    # structure. Additional columns (e.g. upload_id) may be added later.
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT,
            email TEXT,
            username TEXT,
            password TEXT,
            base_domain TEXT,
            account TEXT,
            date_created TEXT DEFAULT (datetime('now')),
            upload_id INTEGER
        );
        """
    )
    # Create uploads table if not exists. This table stores metadata
    # about each file that has been processed, including the timestamp
    # of the upload, the original file name and an optional quality
    # ranking. Quality may be any arbitrary string (e.g. 'regular',
    # 'high quality', etc.).
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_timestamp TEXT,
            file_name TEXT,
            quality TEXT
        );
        """
    )
    # Basic indexes to speed up common queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_url ON logs (url);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_email ON logs (email);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_username ON logs (username);")
    # Do not create the upload_id index yet; it will be added after the
    # schema upgrade section once we are sure the upload_id column exists.
    conn.commit()

    # After ensuring the tables exist, handle schema upgrades and deduplication.
    try:
        # Fetch existing column names to determine if any schema upgrades are needed
        cursor.execute("PRAGMA table_info(logs);")
        existing_cols = [row[1] for row in cursor.fetchall()]
        # Add base_domain column if missing (for older databases)
        if 'base_domain' not in existing_cols:
            cursor.execute("ALTER TABLE logs ADD COLUMN base_domain TEXT;")
            conn.commit()
        # Add account column if missing
        if 'account' not in existing_cols:
            cursor.execute("ALTER TABLE logs ADD COLUMN account TEXT;")
            conn.commit()
        # Add upload_id column if missing
        if 'upload_id' not in existing_cols:
            cursor.execute("ALTER TABLE logs ADD COLUMN upload_id INTEGER;")
            conn.commit()
        # Create an index on base_domain if one does not exist
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_base_domain ON logs (base_domain);")
        # Deduplicate any existing records based on (base_domain, account, password).
        # Keep the earliest record (lowest id) for each group and delete the rest.
        cursor.execute(
            """
            DELETE FROM logs
            WHERE id NOT IN (
                SELECT MIN(id) FROM logs
                GROUP BY base_domain, account, password
            );
            """
        )
        conn.commit()
        # Create a unique index on (base_domain, account, password) to prevent
        # duplicates across subdomains of the same registrable domain. If the
        # index creation fails due to existing duplicates or concurrent
        # access, we ignore the error.
        try:
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_logs_unique ON logs (base_domain, account, password);"
            )
        except sqlite3.OperationalError:
            pass
        # Create an index on upload_id after ensuring the column exists. The
        # upload_id column might have been added above. Wrapping in a try
        # clause prevents errors if the column does not exist (e.g. during
        # concurrent schema migrations). If the column is missing the
        # index creation will fail and be ignored.
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_upload_id ON logs (upload_id);")
        except sqlite3.OperationalError:
            pass
        conn.commit()
    except sqlite3.OperationalError:
        # Log table might not exist or other concurrency error; ignore.
        pass
    conn.close()


def insert_record(
    conn: sqlite3.Connection,
    record: Tuple[str, str, str, str, str, str],
    upload_id: Optional[int] = None,
) -> None:
    """Insert a single record into the database.

    The record includes a base_domain and account component used for
    deduplication, and optionally an upload_id linking it to an entry
    in the uploads table. Insertion is performed using ``INSERT OR
    IGNORE`` so that if a duplicate record (same base_domain, account
    and password) already exists, the new entry is skipped.

    Parameters
    ----------
    conn: sqlite3.Connection
        An open SQLite database connection.
    record: Tuple[str, str, str, str, str, str]
        A tuple containing (url, email, username, password, base_domain, account).
    upload_id: Optional[int]
        The ID of the upload this record belongs to. If provided, the
        record will reference the corresponding row in the uploads table.
    """
    cursor = conn.cursor()
    if upload_id is None:
        cursor.execute(
            """
            INSERT OR IGNORE INTO logs (
                url, email, username, password, base_domain, account
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            record,
        )
    else:
        cursor.execute(
            """
            INSERT OR IGNORE INTO logs (
                url, email, username, password, base_domain, account, upload_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            record + (upload_id,),
        )

def create_upload(db_path: str, file_name: str, quality: str = "regular") -> int:
    """Create a new upload record in the uploads table.

    Each time a file is processed, a new entry is created in the
    ``uploads`` table recording the timestamp of the upload, the
    original file name and the associated quality ranking. The
    returned ID can be passed to :func:`process_file` or
    :func:`process_file_with_progress` so that all parsed records
    reference this upload.

    Parameters
    ----------
    db_path: str
        Path to the SQLite database.
    file_name: str
        Name of the file being uploaded. This is stored for
        informational purposes.
    quality: str, optional
        A quality ranking for this upload (e.g. 'regular', 'good',
        'high quality', etc.). Defaults to 'regular'.

    Returns
    -------
    int
        The ID of the newly created upload record.
    """
    # Open a new connection with a longer timeout and set a busy timeout to
    # handle concurrent writes. Without setting PRAGMA busy_timeout, SQLite
    # raises OperationalError immediately if the database is locked. A 30
    # second timeout allows this call to wait for existing transactions to
    # complete before raising an error.
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        # Wait up to 30 seconds for busy locks to clear
        try:
            conn.execute("PRAGMA busy_timeout = 30000;")
        except sqlite3.OperationalError:
            pass
        cursor = conn.cursor()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            "INSERT INTO uploads (upload_timestamp, file_name, quality) VALUES (?, ?, ?)",
            (timestamp, file_name, quality),
        )
        upload_id = cursor.lastrowid
        conn.commit()
    finally:
        conn.close()
    return upload_id


def list_uploads(db_path: str) -> "list[tuple]":
    """Return a list of all uploads sorted by most recent first.

    Each tuple contains (id, upload_timestamp, file_name, quality).

    Parameters
    ----------
    db_path: str
        Path to the SQLite database.

    Returns
    -------
    list[tuple]
        A list of uploads ordered by descending upload_timestamp.
    """
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        # Set busy timeout to allow waiting for locks to clear before reading
        try:
            conn.execute("PRAGMA busy_timeout = 30000;")
        except sqlite3.OperationalError:
            pass
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, upload_timestamp, file_name, quality FROM uploads ORDER BY upload_timestamp DESC"
        )
        result = cursor.fetchall()
    finally:
        conn.close()
    return result


def update_upload_quality(db_path: str, upload_id: int, quality: str) -> None:
    """Update the quality ranking of an existing upload.

    Parameters
    ----------
    db_path: str
        Path to the SQLite database.
    upload_id: int
        ID of the upload to update.
    quality: str
        New quality value.
    """
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        # Use a busy timeout to wait if the database is locked by another write.
        try:
            conn.execute("PRAGMA busy_timeout = 30000;")
        except sqlite3.OperationalError:
            pass
        cursor = conn.cursor()
        cursor.execute("UPDATE uploads SET quality = ? WHERE id = ?", (quality, upload_id))
        conn.commit()
    finally:
        conn.close()

def process_lines(
    lines: Iterable[str],
    conn: sqlite3.Connection,
    upload_id: Optional[int] = None,
) -> int:
    """Process an iterable of lines, inserting parsed records into the database.

    Parameters
    ----------
    lines: Iterable[str]
        A stream or iterable of raw log lines.
    conn: sqlite3.Connection
        An open SQLite database connection. The caller is responsible
        for closing this connection.

    Returns
    -------
    int
        The number of records successfully inserted.
    """
    count = 0
    for raw in lines:
        cleaned = clean_line(raw)
        if not cleaned:
            continue
        parsed = parse_line(cleaned)
        if parsed is None:
            continue
        url, email, username, password = parsed
        # Coalesce None values to empty strings so that the unique index
        # treats missing values consistently when deduplicating. SQLite treats
        # NULL values as distinct in UNIQUE constraints, so using empty strings
        # prevents duplicate rows when fields are missing.
        email = email or ''
        username = username or ''
        password = password or ''
        # Compute the base domain for deduplication purposes. An empty string is
        # used if no URL is available.
        base_domain = extract_base_domain(url) if url else ''
        # Determine the account identifier used for deduplication: email
        # takes precedence; if email is empty, fall back to username.
        account = email if email else username
        insert_record(conn, (url or '', email, username, password, base_domain, account), upload_id)
        count += 1
    # Commit after processing a batch of lines to reduce locking overhead.
    conn.commit()
    return count


def process_file(
    path: str,
    db_path: str,
    chunk_size: int = CHUNK_SIZE_BYTES,
    upload_id: Optional[int] = None,
) -> int:
    """Process a potentially large file, reading it in chunks.

    This function opens the log file at ``path``, reads it in chunks of
    ``chunk_size`` bytes and streams the lines to :func:`process_lines`.
    It ensures that lines spanning chunk boundaries are handled correctly.

    Parameters
    ----------
    path: str
        Absolute or relative path to the log file.
    db_path: str
        Path to the SQLite database where records will be inserted.
    chunk_size: int, optional
        The maximum number of bytes to read at once. Default is 100 MB.

    Returns
    -------
    int
        The total number of records inserted across all chunks.
    """
    total_inserted = 0
    init_database(db_path)
    # Open the connection with a longer timeout to reduce locking errors.
    conn = sqlite3.connect(db_path, timeout=30)
    # Ensure the busy timeout is set on this connection as well.
    conn.execute("PRAGMA busy_timeout = 30000;")
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            leftover = ""
            while True:
                chunk = fh.read(chunk_size)
                if not chunk:
                    break
                # Prepend any leftover from the previous chunk
                data = leftover + chunk
                lines = data.splitlines(keepends=False)
                # If the last line does not end with a newline, keep it as leftover
                if chunk and not chunk.endswith("\n"):
                    leftover = lines.pop() if lines else ""
                else:
                    leftover = ""
                total_inserted += process_lines(lines, conn, upload_id)
            # Process any remaining line after the loop
            if leftover:
                total_inserted += process_lines([leftover], conn, upload_id)
    finally:
        conn.close()
    return total_inserted


def process_file_with_progress(
    path: str,
    db_path: str,
    progress_callback: Optional[callable] = None,
    chunk_size: int = CHUNK_SIZE_BYTES,
    upload_id: Optional[int] = None,
) -> int:
    """Process a log file with progress reporting.

    This function behaves like :func:`process_file` but calls
    ``progress_callback`` after processing each chunk of the file. The
    callback receives three arguments: the cumulative number of lines
    processed so far, the total number of records inserted so far, and
    the fraction of the file (in bytes) that has been read so far.

    Parameters
    ----------
    path: str
        Path to the log file to process.
    db_path: str
        Path to the SQLite database where records will be inserted.
    progress_callback: callable, optional
        A function called after each chunk is processed. It should
        accept three positional arguments:

        * ``lines_processed``: int — the total number of lines processed
          (including duplicates and discarded lines).
        * ``records_inserted``: int — the total number of database
          records inserted so far.
        * ``progress_fraction``: float — the fraction of the file size
          processed so far (0–1).
    chunk_size: int, optional
        The maximum number of bytes to read at once. Default is
        ``CHUNK_SIZE_BYTES`` (100 MB).

    Returns
    -------
    int
        The total number of records inserted across all chunks.
    """
    total_inserted = 0
    lines_processed = 0
    # The database structure should already be initialised by ensure_db() in the
    # UI code. Avoid calling init_database() here to reduce the chance of
    # locking the database while another connection is writing.
    # Open the connection with a longer timeout to reduce locking errors.
    conn = sqlite3.connect(db_path, timeout=30)
    # Ensure the busy timeout is set on this connection as well.
    conn.execute("PRAGMA busy_timeout = 30000;")
    try:
        file_size = None
        try:
            file_size = os.path.getsize(path)
        except Exception:
            file_size = None
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            leftover = ""
            while True:
                chunk = fh.read(chunk_size)
                if not chunk:
                    break
                # Prepend any leftover from the previous chunk
                data = leftover + chunk
                lines = data.splitlines(keepends=False)
                # If the last line does not end with a newline, keep it as leftover
                if chunk and not chunk.endswith("\n"):
                    leftover = lines.pop() if lines else ""
                else:
                    leftover = ""
                # Process lines and update counters
                total_inserted += process_lines(lines, conn, upload_id)
                lines_processed += len(lines)
                # Compute progress fraction based on file size if available
                progress_fraction = 0.0
                if file_size and file_size > 0:
                    try:
                        # fh.tell() reports the number of characters read from
                        # the file. Multiplying by the average bytes per
                        # character is unnecessary here because the fraction
                        # still provides a monotonically increasing measure of
                        # progress relative to file_size.
                        progress_fraction = min(fh.tell() / file_size, 1.0)
                    except Exception:
                        progress_fraction = 0.0
                # Invoke the callback if provided
                if progress_callback:
                    try:
                        progress_callback(lines_processed, total_inserted, progress_fraction)
                    except Exception:
                        pass
            # Process any remaining line after the loop
            if leftover:
                total_inserted += process_lines([leftover], conn, upload_id)
                lines_processed += 1
                if progress_callback:
                    try:
                        if file_size and file_size > 0:
                            progress_fraction = 1.0
                        progress_callback(lines_processed, total_inserted, progress_fraction)
                    except Exception:
                        pass
    finally:
        conn.close()
    return total_inserted


def query_database(
    db_path: str,
    search: str = "",
    sort_by: str = "id",
    ascending: bool = True,
    upload_ids: Optional[Iterable[int]] = None,
    limit: Optional[int] = None,
) -> "list[tuple]":
    """Query the database with optional search and sorting parameters.

    This function returns a list of rows matching the search string
    (case insensitive) across the url, email or username fields. Results
    can be sorted by a specified column.

    Parameters
    ----------
    db_path: str
        Path to the SQLite database.
    search: str, optional
        A search string to filter records. Matches against URL, email
        or username. Default is empty string (no filtering).
    sort_by: str, optional
        Column name to sort by. Must be one of 'id', 'url', 'email',
        'username', 'password', 'date_created'. Default is 'id'.
    ascending: bool, optional
        Whether to sort in ascending order. Default True.

    Returns
    -------
    list[tuple]
        List of matching rows.
    """
    # Only allow sorting by columns included in the SELECT clause. The base_domain
    # column is intentionally excluded from results and thus not a valid sort key.
    allowed_sort_fields = {'id', 'url', 'email', 'username', 'password', 'date_created', 'upload_id'}
    if sort_by not in allowed_sort_fields:
        sort_by = 'id'
    order = 'ASC' if ascending else 'DESC'
    conn = sqlite3.connect(db_path, timeout=30)
    # Set busy timeout to allow waiting for locks to clear
    try:
        conn.execute("PRAGMA busy_timeout = 30000;")
    except sqlite3.OperationalError:
        pass
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    # Build base query including the upload_id column so that callers can
    # filter or display based on which upload a record belongs to.
    base_query = f"SELECT id, url, email, username, password, date_created, upload_id FROM logs"
    # Build WHERE conditions
    where_clauses = []
    params = []
    if search:
        pattern = f"%{search}%"
        where_clauses.append("(url LIKE ? OR email LIKE ? OR username LIKE ?)")
        params.extend([pattern, pattern, pattern])
    # Filter by upload_ids if provided. If a non-None iterable is provided
    # but is empty, return no rows immediately because an empty IN ()
    # clause is invalid SQL. This way the caller receives an empty
    # result set without error.
    if upload_ids is not None:
        upload_ids_list = list(upload_ids)
        if not upload_ids_list:
            conn.close()
            return []
        placeholders = ",".join(["?"] * len(upload_ids_list))
        where_clauses.append(f"(upload_id IN ({placeholders}))")
        params.extend(upload_ids_list)
    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)
    # Compose the SQL statement. Add a LIMIT clause if provided to
    # avoid loading very large result sets into memory all at once.
    query = f"{base_query}{where_sql} ORDER BY {sort_by} {order}"
    if limit is not None and limit > 0:
        query += f" LIMIT {int(limit)}"
    query += ";"
    cursor.execute(query, tuple(params))
    rows = cursor.fetchall()
    conn.close()
    return [tuple(row) for row in rows]


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Parse log files into an SQLite database.")
    parser.add_argument("logfile", help="Path to the log file to process")
    parser.add_argument("database", help="Path to the SQLite database to create or append to")
    args = parser.parse_args()
    print(f"Processing {args.logfile} into database {args.database}...")
    inserted = process_file(args.logfile, args.database)
    print(f"Inserted {inserted} records into {args.database}")