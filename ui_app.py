"""
ui_app.py
----------

This Streamlit application provides a simple web interface to the log
parser defined in :mod:`log_parser`. It allows users to upload large
log files, parse them into an SQLite database, perform interactive
searching and sorting, run optional URL validity checks and export
selected fields to a text file.

To run the application locally, install the required dependencies and
execute ``streamlit run ui_app.py`` from the command line. The app will
start a local web server that can be accessed via a browser.

Note that for very large files (up to several gigabytes) the upload
process may take some time. Files are processed in 100 MB chunks to
limit memory usage.
"""

import os
import tempfile
from typing import List

import pandas as pd
import requests
import streamlit as st
from datetime import datetime, timedelta

# Define a maximum number of rows to retrieve from the database in a single query.
# Fetching extremely large result sets can lead to MemoryError exceptions. This
# limit ensures that we do not load more than this many rows into memory at once.
MAX_QUERY_ROWS = 200_000

import log_parser


# Path to the SQLite database used by the application. If the file does
# not exist it will be created automatically when a file is processed.
DB_PATH = os.path.join(tempfile.gettempdir(), "parsed_logs.db")


def ensure_db() -> None:
    """Ensure the database is initialised and up to date.

    Initialise the database schema only once per session. Using
    ``st.session_state``, we track whether the database has been
    initialised to avoid repeated calls to :func:`log_parser.init_database`,
    which can cause the database to become locked if invoked from
    multiple threads concurrently. ``init_database`` is still
    idempotent, but limiting its invocation reduces contention.
    """
    if not st.session_state.get("db_initialized", False):
        # Initialise the database schema. This call will create tables
        # and perform migrations if necessary. It is safe to call
        # repeatedly, but we avoid doing so to prevent lock contention.
        log_parser.init_database(DB_PATH)
        st.session_state["db_initialized"] = True


def validate_url(url: str, timeout: float = 5.0) -> bool:
    """Check whether a given URL responds with HTTP status 200.

    The URL stored in the database does not include a protocol prefix
    (http/https). This function attempts to contact the URL using
    ``https`` and falls back to ``http`` if the first attempt fails.

    Parameters
    ----------
    url: str
        The URL without protocol to validate.
    timeout: float
        Timeout in seconds for the HTTP request. Default is 5 seconds.

    Returns
    -------
    bool
        ``True`` if a request to the URL returns status code 200,
        ``False`` otherwise.
    """
    # Remove leading slashes to avoid duplicated protocol separators (e.g.
    # ``//example.com`` → ``example.com``). If the URL is empty after
    # stripping, it is considered invalid.
    stripped = url.lstrip("/")
    if not stripped:
        return False
    for scheme in ("https://", "http://"):
        full_url = f"{scheme}{stripped}"
        try:
            resp = requests.head(full_url, timeout=timeout)
            if resp.status_code == 200:
                return True
        except requests.RequestException:
            continue
    return False


def main() -> None:
    """Run the Streamlit application."""
    st.set_page_config(page_title="Log Parser UI", layout="wide")
    st.title("Log File Parser and Manager")
    ensure_db()

    # Define the available quality rankings for uploads. Users can assign
    # one of these values when processing files and later filter by
    # quality. Feel free to adjust or extend this list.
    quality_options: List[str] = [
        "big old", "regular", "good", "high quality", "ultra high quality"
    ]

    # File upload section
    st.header("Upload or process log files")
    uploaded_files = st.file_uploader(
        "Select one or more log files (plain text)", type=None, accept_multiple_files=True
    )
    if uploaded_files:
        for idx, uploaded_file in enumerate(uploaded_files):
            st.write(f"Processing **{uploaded_file.name}**...")
            # Allow the user to choose a quality ranking for this upload. A unique
            # key is provided to ensure each selectbox retains its state when
            # multiple files are uploaded.
            quality = st.selectbox(
                f"Select quality ranking for {uploaded_file.name}",
                options=quality_options,
                index=1,
                key=f"quality_{idx}_{uploaded_file.name}",
            )
            # Write to a temporary file on disk to allow chunked processing
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getbuffer())
                tmp_path = tmp.name
            # Create a placeholder for a custom progress bar. This placeholder will
            # render a simple HTML-based progress bar that updates dynamically
            # without interfering with the layout.
            progress_placeholder = st.empty()

            def update_progress(lines_processed: int, records_inserted: int, fraction: float) -> None:
                """Update the progress bar and status message.

                The progress bar is rendered using a simple HTML div with
                adjustable width. This avoids issues where Streamlit's built-in
                progress bar can be cut off or interfere with page scrolling.
                """
                fraction = min(max(fraction, 0.0), 1.0)
                bar_width = fraction * 100
                progress_bar_html = f"""
                <div style='width: 100%; background-color: #444; border-radius: 5px; overflow: hidden;'>
                  <div style='width: {bar_width:.2f}%; height: 20px; background-color: #4CAF50;'></div>
                </div>
                <p style='margin-top: 4px;'>Processed {lines_processed:,} lines, inserted {records_inserted:,} records...</p>
                """
                progress_placeholder.markdown(progress_bar_html, unsafe_allow_html=True)

            # Create a new upload record and obtain its ID
            upload_id = log_parser.create_upload(DB_PATH, uploaded_file.name, quality)
            # Process the file with progress callback, associating all records
            # with the newly created upload. The upload_id is passed through
            # to ensure each record references the correct upload.
            inserted = log_parser.process_file_with_progress(
                tmp_path,
                DB_PATH,
                progress_callback=update_progress,
                upload_id=upload_id,
            )
            # Final update to indicate completion
            update_progress(lines_processed=0, records_inserted=inserted, fraction=1.0)
            st.success(f"Inserted {inserted} records from {uploaded_file.name}.")
            os.remove(tmp_path)

    # Alternative processing: allow specifying a local file path directly. This
    # bypasses the Streamlit file_uploader limit (200 MB by default) by
    # reading the file from disk on the server. Only use this option if
    # you are running the app locally and the file is accessible to the
    # server's filesystem.
    st.subheader("Or process a file by specifying its path")
    file_path_input = st.text_input(
        "Enter the full path to a log file on the server (use this for very large files):",
        ""
    )
    # Allow the user to choose a quality ranking for the file to be processed via its path.
    quality_path = st.selectbox(
        "Select quality ranking for this file", options=quality_options, index=1, key="quality_path"
    )
    if st.button("Process File Path"):
        if file_path_input:
            if os.path.exists(file_path_input) and os.path.isfile(file_path_input):
                st.write(f"Processing **{file_path_input}**...")
                # Use the quality ranking selected above for this file
                quality = quality_path
                # Create a placeholder for a custom progress bar that updates
                # dynamically without interfering with the layout.
                progress_placeholder = st.empty()

                def update_progress(lines_processed: int, records_inserted: int, fraction: float) -> None:
                    fraction = min(max(fraction, 0.0), 1.0)
                    bar_width = fraction * 100
                    progress_bar_html = f"""
                    <div style='width: 100%; background-color: #444; border-radius: 5px; overflow: hidden;'>
                      <div style='width: {bar_width:.2f}%; height: 20px; background-color: #4CAF50;'></div>
                    </div>
                    <p style='margin-top: 4px;'>Processed {lines_processed:,} lines, inserted {records_inserted:,} records...</p>
                    """
                    progress_placeholder.markdown(progress_bar_html, unsafe_allow_html=True)

                try:
                    # Create a new upload record and obtain its ID
                    file_name = os.path.basename(file_path_input)
                    upload_id = log_parser.create_upload(DB_PATH, file_name, quality)
                    inserted = log_parser.process_file_with_progress(
                        file_path_input,
                        DB_PATH,
                        progress_callback=update_progress,
                        upload_id=upload_id,
                    )
                    # Final update to indicate completion
                    update_progress(lines_processed=0, records_inserted=inserted, fraction=1.0)
                    st.success(f"Inserted {inserted} records from {file_path_input}.")
                except Exception as exc:
                    st.error(f"Failed to process {file_path_input}: {exc}")
            else:
                st.error("The specified file does not exist or is not a file. Please check the path and try again.")
        else:
            st.warning("Please enter a valid file path before clicking 'Process File Path'.")

    # Search and sort section
    st.header("Browse Parsed Records")
    with st.form(key="search_form"):
        search_term = st.text_input("Search (matches URL, email or username)", "")
        sort_by = st.selectbox(
            "Sort by", options=["id", "url", "email", "username", "password", "date_created", "upload_id"], index=0
        )
        ascending = st.checkbox("Ascending order", value=True)
        # Option to exclude common personal email domains from the results
        exclude_personal = st.checkbox(
            "Exclude personal email domains (icloud, hotmail, live, gmail, yahoo, protonmail, aol, outlook)",
            value=False
        )
        # Option to display only the root domain (base domain) for URLs
        root_only = st.checkbox(
            "Show root domain only for URLs", value=False
        )
        # Filter by upload time. Users can choose to view only records from the
        # most recent upload, uploads within the past 24 hours, the past week,
        # or all uploads. The timestamps are stored in the uploads table.
        upload_time_filter = st.selectbox(
            "Upload time filter",
            options=["All uploads", "Most recent upload", "Past 24 hours", "Past week"],
            index=0,
        )
        # Filter by quality ranking. Users can select one or more quality
        # categories to include. Leaving this empty includes all qualities.
        quality_filter = st.multiselect(
            "Filter by quality ranking", options=quality_options, default=[], key="quality_filter"
        )
        # Advanced filtering: allow up to three independent column filters.
        with st.expander("Advanced Filters (optional)", expanded=False):
            filter_columns: List[str] = []
            filter_operations: List[str] = []
            filter_values: List[str] = []
            for i in range(3):
                col = st.selectbox(
                    f"Filter {i + 1} column",
                    options=["url", "email", "username", "password", "date_created"],
                    index=0,
                    key=f"adv_col_{i}",
                )
                op = st.selectbox(
                    f"Filter {i + 1} operation",
                    options=["Contains", "Does not contain"],
                    index=0,
                    key=f"adv_op_{i}",
                )
                val = st.text_input(
                    f"Filter {i + 1} value", "", key=f"adv_val_{i}"
                )
                filter_columns.append(col)
                filter_operations.append(op)
                filter_values.append(val)
        submitted = st.form_submit_button("Apply Filters")

    # Query database and display results. Before querying, determine whether
    # filtering by upload time or quality is necessary. If so, compute the
    # relevant upload IDs. Otherwise set upload_ids to None to fetch all records.
    upload_ids = None
    try:
        uploads = log_parser.list_uploads(DB_PATH)
        # Build the set of upload IDs based on time filter
        ids_by_time: set[int] = set()
        # If there are uploads present, compute the sets based on the selected filter
        if uploads:
            # Convert timestamp strings to datetime objects
            parsed_uploads = []
            for (uid, ts_str, fname, qual) in uploads:
                try:
                    ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    ts_dt = datetime.min
                parsed_uploads.append((uid, ts_dt, fname, qual))
            if upload_time_filter == "Most recent upload":
                # The uploads list is ordered by descending timestamp, so take the first one
                most_recent_id = parsed_uploads[0][0] if parsed_uploads else None
                if most_recent_id is not None:
                    ids_by_time = {most_recent_id}
            elif upload_time_filter == "Past 24 hours":
                threshold = datetime.now() - timedelta(hours=24)
                ids_by_time = {uid for (uid, ts_dt, _, _) in parsed_uploads if ts_dt >= threshold}
            elif upload_time_filter == "Past week":
                threshold = datetime.now() - timedelta(days=7)
                ids_by_time = {uid for (uid, ts_dt, _, _) in parsed_uploads if ts_dt >= threshold}
            else:
                # All uploads: include all upload IDs
                ids_by_time = {uid for (uid, _, _, _) in parsed_uploads}
        # Build set based on quality filter
        ids_by_quality: set[int] = set()
        if quality_filter:
            ids_by_quality = {uid for (uid, _, _, qual) in uploads if qual in quality_filter}
        # Combine filters
        if upload_time_filter != "All uploads" or quality_filter:
            # Start from all ids if one of the filters is empty; else intersect
            if quality_filter and (upload_time_filter != "All uploads"):
                # Both filters apply
                ids = ids_by_time & ids_by_quality
            elif quality_filter:
                ids = ids_by_quality
            elif upload_time_filter != "All uploads":
                ids = ids_by_time
            else:
                ids = set()
            # If there are ids, set upload_ids, otherwise use empty list to get no results
            upload_ids = list(ids)
    except Exception:
        # If listing uploads fails, fall back to no filtering by upload
        upload_ids = None

    rows = log_parser.query_database(
        DB_PATH,
        search=search_term,
        sort_by=sort_by,
        ascending=ascending,
        upload_ids=upload_ids,
        limit=MAX_QUERY_ROWS,
    )
    if rows:
        # If the number of rows returned equals the maximum query limit, it is
        # possible that the result set was truncated. Notify the user so they
        # can refine their search or filters to narrow down the results.
        if len(rows) >= MAX_QUERY_ROWS:
            st.warning(
                f"The query returned at least {MAX_QUERY_ROWS} rows and has been truncated. "
                "Refine your search or filters to obtain a smaller result set."
            )

        # Include upload_id column in the DataFrame for filtering. This column
        # can be dropped before display if desired.
        df = pd.DataFrame(
            rows,
            columns=["id", "url", "email", "username", "password", "date_created", "upload_id"],
        )
        # Apply personal email filter if requested
        if exclude_personal:
            personal_domains = {
                "icloud.com",
                "hotmail.com",
                "live.com",
                "gmail.com",
                "yahoo.com",
                "protonmail.com",
                "aol.com",
                "outlook.com",
            }

            def is_personal(email: str) -> bool:
                if not email or "@" not in email:
                    return False
                return email.lower().split("@")[-1] in personal_domains

            df = df[~df["email"].apply(is_personal)]

        # If requested, replace the URL with its base domain for display and export
        try:
            if root_only:
                df["url"] = df["url"].apply(log_parser.extract_base_domain)
        except Exception:
            # If extraction fails, silently ignore and continue with full URL
            pass

        # Apply advanced filters. Iterate through each configured filter and
        # refine the DataFrame accordingly. Filters with empty values are
        # skipped. Each filter requires both a column and a non-empty value.
        try:
            for col, op, val in zip(filter_columns, filter_operations, filter_values):
                if val:
                    if col in df.columns:
                        series = df[col].astype(str).fillna("")
                        if op == "Contains":
                            mask = series.str.contains(val, case=False, na=False)
                        else:
                            mask = ~series.str.contains(val, case=False, na=False)
                        df = df[mask]
        except Exception:
            # Ignore filter errors (e.g. invalid column) and continue
            pass

        # Drop the upload_id column from the DataFrame before display and export.
        # The column is retained in the underlying DataFrame for filtering but
        # not shown to the user.
        if "upload_id" in df.columns:
            df_display_base = df.drop(columns=["upload_id"])
        else:
            df_display_base = df

        # Limit the amount of data sent to the browser to avoid Streamlit
        # MessageSizeError for very large datasets. Compute the approximate
        # memory footprint of the DataFrame and cap the number of rows
        # displayed if it exceeds a threshold. Users can still export
        # the full dataset via the export controls below.
        max_display_rows = 50000
        # Calculate DataFrame size in megabytes
        df_size_mb = df.memory_usage(deep=True).sum() / (1024 ** 2)
        if df_size_mb > 50:
            # Large dataset: display only a subset of rows to avoid large
            # payloads being sent to the browser. Notify the user about
            # the truncation.
            st.warning(
                f"The result set is large (~{df_size_mb:.1f} MB). Only the first {max_display_rows} rows "
                "are displayed. Use the export function to download the full dataset."
            )
            display_df = df_display_base.head(max_display_rows)
        else:
            display_df = df_display_base
        st.dataframe(display_df, use_container_width=True)

        # URL validation and export controls
        st.subheader("Actions")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Validate URLs"):
                st.write("Checking validity of each URL...")
                # Add a new column indicating validity
                df["valid"] = df["url"].apply(validate_url)
                # Display results without the upload_id column
                df_valid_display = df.drop(columns=["upload_id"])
                st.dataframe(df_valid_display, use_container_width=True)
        with col2:
            # Column selection for export
            export_cols = st.multiselect(
                "Select columns to export",
                options=["url", "email", "username", "password", "date_created"],
                default=["url", "email", "username", "password"],
            )
            delimiter = st.selectbox(
                "Choose a delimiter for export",
                options=[":", ",", ";", "|", "CSV (spreadsheet)"]
            )
            if export_cols:
                # Use the DataFrame without the upload_id column for export
                export_df = df_display_base[export_cols]
                if delimiter == "CSV (spreadsheet)":
                    # Export to CSV with header row
                    export_content = export_df.to_csv(index=False)
                    mime_type = "text/csv"
                    filename = "export.csv"
                else:
                    sep = delimiter
                    export_content = "\n".join(
                        sep.join(str(val) if val is not None else "" for val in row)
                        for row in export_df.itertuples(index=False, name=None)
                    )
                    mime_type = "text/plain"
                    filename = "export.txt"
                st.download_button(
                    label="Download Export",
                    data=export_content,
                    file_name=filename,
                    mime=mime_type,
                )
    else:
        st.info("No records match your query or database is empty.")

    # Section to manage uploads: view recent uploads and assign quality rankings.
    st.header("Manage Uploads")
    try:
        uploads_list = log_parser.list_uploads(DB_PATH)
        if uploads_list:
            # Display the list of uploads as a table for reference
            uploads_df = pd.DataFrame(
                uploads_list,
                columns=["upload_id", "upload_timestamp", "file_name", "quality"],
            )
            st.dataframe(uploads_df, use_container_width=True)
            # Provide controls to update the quality of an existing upload
            # Build a dictionary of display names to upload IDs
            upload_option_map = {
                f"{row[1]} - {row[2]} (current: {row[3]})": row[0] for row in uploads_list
            }
            selected_label = st.selectbox(
                "Select an upload to modify its quality", options=list(upload_option_map.keys())
            )
            new_quality = st.selectbox(
                "Select new quality ranking", options=quality_options, index=1
            )
            if st.button("Update Quality"):
                try:
                    upload_id_to_update = upload_option_map.get(selected_label)
                    log_parser.update_upload_quality(DB_PATH, upload_id_to_update, new_quality)
                    st.success("Upload quality updated successfully.")
                except Exception as e:
                    st.error(f"Failed to update quality: {e}")
        else:
            st.info("No uploads have been processed yet.")
    except Exception as e:
        st.error(f"Failed to load uploads: {e}")


if __name__ == "__main__":
    main()