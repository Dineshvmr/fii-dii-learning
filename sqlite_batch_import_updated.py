import sqlite3
import json
import os
import psycopg2
from getpass import getpass

host = "stagingdb.int.sensibull.com"
port = "5432"
dbname = "sensibull_staging"
user = "sensibull_staging"
password = getpass("Enter database password: ")

dest_db = '/Users/dinesh/Documents/SQLite/test'
batch_size = 5
progress_file = 'batch_progress.json'

def get_progress():
    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                return json.load(f)
        return {'tv_drawings': 0, 'tv_study_templates': 0}
    except Exception as e:
        print(f"Error loding progress: {e}")
        raise e

def save_progress(progress):
    try:
        with open(progress_file, 'w') as f:
            json.dump(progress, f, indent=4)
    except Exception as e:
        print(f"Error saving progress: {e}")
        raise e

def process_table(table, progress):
    conn_s = None
    conn_d = None
    try:
        conn_s = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        conn_d = sqlite3.connect(dest_db)

        # Create tables with matching schemas including id as PK
        if table == 'tv_drawings':
            conn_d.execute('''
                CREATE TABLE IF NOT EXISTS tv_drawings (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    content BLOB NOT NULL
                )
            ''')
        elif table == 'tv_study_templates':
            conn_d.execute('''
                CREATE TABLE IF NOT EXISTS tv_study_templates (
                    id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    content BLOB NOT NULL
                )
            ''')
        else:
            print(f"Unknown table {table}")
            return

        # Get total count
        cursor = conn_s.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total = cursor.fetchone()[0]
        cursor.close()
        if total == 0:
            print(f"No data in {table}")
            return

        total_batches = (total + batch_size - 1) // batch_size
        last_batch = progress.get(table)
        print(progress)
        if last_batch is None:
            raise RuntimeError(f"Last batch not found in table {table}")
        
        print(f"Processing {table}: total rows {total}, batches {total_batches}, starting from batch {last_batch}")

        # Define select and insert based on table
        if table == 'tv_drawings':
            select_cols = 'id, user_id, content'
            insert_sql = 'INSERT INTO tv_drawings (id, user_id, content) VALUES (?, ?, ?)'
        elif table == 'tv_study_templates':
            select_cols = 'id, user_id, content'
            insert_sql = 'INSERT INTO tv_study_templates (id, user_id, content) VALUES (?, ?, ?)'
        else:
            print(f"Unknown table {table}")
            return

        for batch in range(last_batch, total_batches):
            offset = batch * batch_size
            cursor = conn_s.cursor()
            cursor.execute(
                f"SELECT {select_cols} FROM {table} ORDER BY id LIMIT %s OFFSET %s",
                (batch_size, offset)
            )
            rows = cursor.fetchall()
            cursor.close()

            if not rows:
                break

            print(f"Fetched {len(rows)} rows for batch {batch + 1}")
            # Use executemany for batch insert
            conn_d.executemany(insert_sql, rows)
            inserted_count = conn_d.total_changes
            conn_d.commit()

            print(f"Total rows inserted after batch {batch + 1}: {inserted_count}")

            progress[table] = batch + 1
            save_progress(progress)
            print(f"Completed batch {batch + 1}/{total_batches} for {table} ({len(rows)} rows)")

        print(f"Finished processing {table}")
    except Exception as e:
        print(f"Error processing {table}: {e}")
    finally:
        if conn_s:
            conn_s.close()
        if conn_d:
            conn_d.close()

if __name__ == "__main__":
    progress = get_progress()
    for table in ['tv_drawings', 'tv_study_templates']:
        process_table(table, progress)
    print("Batch import completed. Progress saved in batch_progress.json")
