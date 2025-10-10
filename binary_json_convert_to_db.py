import sqlite3
import json
import lz4.block
import sys
import os

def deep_parse_json(obj):
    if isinstance(obj, dict):
        return {k: deep_parse_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deep_parse_json(item) for item in obj]
    if isinstance(obj, str):
        try:
            if obj.startswith(('{', '[')):
                parsed = json.loads(obj)
                return deep_parse_json(parsed)
        except json.JSONDecodeError:
            pass
        return obj
    return obj

def decompress_table(db_path, table_name):
    conn = None
    progress_file = "progress.json"
    
    try:
        # Load progress
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
            last_processed = progress.get(table_name, 0)
        else:
            progress = {}
            last_processed = 0
        
        # Add column if not exists
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN parsed_content TEXT")
            conn.commit()
            print(f"Added parsed_content column to {table_name}")
        except sqlite3.OperationalError:
            print(f"parsed_content column already exists in {table_name}")
        
        batch_size = 100
        while True:
            # Fetch batch
            cursor.execute(
                f"SELECT id, content FROM {table_name} WHERE id > ? ORDER BY id LIMIT ?",
                (last_processed, batch_size)
            )
            rows = cursor.fetchall()
            
            if not rows:
                print(f"No more data to process in {table_name} table.")
                break
            
            batch_success = True
            for row in rows:
                record_id = row[0]
                content_blob = row[1]
                
                if content_blob is None:
                    print(f"ID {record_id}: No content in {table_name}")
                    continue
                
                content_decompressed = None
                try:
                    content_decompressed = lz4.block.decompress(content_blob)
                    print(f"Successfully decompressed {table_name} ID {record_id} with LZ4")
                except Exception as e:
                    print(f"LZ4 decompress failed for {table_name} ID {record_id}: {e}")
                    batch_success = False
                    continue
                
                if content_decompressed:
                    try:
                        content_str = content_decompressed.decode('utf-8')
                        json_data = json.loads(content_str)
                        fully_parsed = deep_parse_json(json_data)
                        
                        # Update DB
                        cursor.execute(
                            f"UPDATE {table_name} SET parsed_content = ? WHERE id = ?",
                            (json.dumps(fully_parsed), record_id)
                        )
                        print(f"Updated parsed_content for {table_name} ID {record_id}")
                        
                    except (UnicodeDecodeError, json.JSONDecodeError) as e:
                        print(f"Could not decode/parse {table_name} ID {record_id}: {e}")
                        batch_success = False
                        continue
            
            # Commit batch
            if batch_success:
                conn.commit()
                last_processed = rows[-1][0]
                # Update progress
                progress[table_name] = last_processed
                with open(progress_file, 'w') as f:
                    json.dump(progress, f, indent=2)
                print(f"Processed batch up to ID {last_processed} in {table_name}, progress saved.")
            else:
                print(f"Batch had errors, not committing for {table_name}")
                conn.rollback()
                break
                
    except sqlite3.Error as e:
        print(f"Database error for {table_name}: {e}")
    finally:
        if conn:
            conn.close()
        # Final progress save
        if 'progress' in locals():
            try:
                with open(progress_file, 'w') as f:
                    json.dump(progress, f, indent=2)
                print(f"Final progress saved for {table_name}")
            except Exception as e:
                print(f"Could not save final progress for {table_name}: {e}")

if __name__ == "__main__":
    db_path = "/Users/dinesh/Documents/SQLite/SQlite"
    
    # Process all three tables with LZ4
    print("Processing tv_chart_layouts...")
    decompress_table(db_path, 'tv_chart_layouts')
    
    print("Processing tv_drawings...")
    decompress_table(db_path, 'tv_drawings')
    
    print("Processing tv_study_templates...")
    decompress_table(db_path, 'tv_study_templates')
