import sqlite3
import json
import lz4.block
import argparse
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
    output_dir = f"{table_name}_decompressed"
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Get all records for user_id = 6331
        cursor.execute(f"SELECT id, content FROM {table_name} WHERE user_id = 6331")
        rows = cursor.fetchall()
        
        if not rows:
            print(f"No data found for user 6331 in {table_name} table.")
            return
        
        for row in rows:
            record_id = row[0]
            content_blob = row[1]
            
            if content_blob is None:
                print(f"ID {record_id}: No content for user 6331 in {table_name}")
                continue
            
            content_decompressed = None
            try:
                content_decompressed = lz4.block.decompress(content_blob)
                print(f"Successfully decompressed {table_name} ID {record_id} for user 6331 with LZ4")
            except Exception as e:
                print(f"LZ4 decompress failed for {table_name} ID {record_id}: {e}")
                continue
            
            if content_decompressed:
                try:
                    content_str = content_decompressed.decode('utf-8')
                    json_data = json.loads(content_str)
                    fully_parsed = deep_parse_json(json_data)
                    
                    output_file = os.path.join(output_dir, f"{table_name}_{record_id}_parsed.json")
                    with open(output_file, 'w') as f:
                        json.dump(fully_parsed, f, indent=2)
                    
                    print(f"Saved proper JSON for {table_name} ID {record_id} for user 6331 to {output_file}")
                    
                except (UnicodeDecodeError, json.JSONDecodeError) as e:
                    print(f"Could not decode/parse {table_name} ID {record_id}: {e}")
            
    except sqlite3.Error as e:
        print(f"Database error for {table_name}: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Decompress all records for user 6331 from all three tables using LZ4.")
    parser.add_argument("db_path", help="Path to the SQLite database file")
    args = parser.parse_args()
    
    # Process all three tables with LZ4
    print("Processing tv_chart_layouts for user 6331...")
    decompress_table(args.db_path, 'tv_chart_layouts')
    
    print("Processing tv_drawings for user 6331...")
    decompress_table(args.db_path, 'tv_drawings')
    
    print("Processing tv_study_templates for user 6331...")
    decompress_table(args.db_path, 'tv_study_templates')
