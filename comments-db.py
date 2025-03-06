import sqlite3
import json
import argparse
from tqdm import tqdm

def create_table(cursor):
    """Creates the comments table with only author and link_id."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id TEXT PRIMARY KEY,
            author TEXT,
            link_id TEXT
        )
    """)

def count_lines(filename):
    with open(filename, 'rb') as f:
        return sum(1 for _ in f)

def insert_data(cursor, jsonl_file):
    total_lines = count_lines(jsonl_file)
    with open(jsonl_file, "r", encoding="utf-8") as file:
        for line in tqdm(file, desc="Inserting data", unit="entry", total=total_lines):
            comment = json.loads(line.strip())
            cursor.execute("""
                INSERT OR IGNORE INTO comments 
                (id, author, link_id)
                VALUES (?, ?, ?)
            """, (
                comment.get("id"),
                comment.get("author", "unknown"),
                comment.get("link_id", "unknown")
            ))

def main():
    parser = argparse.ArgumentParser(description="Insert JSONL data into a SQLite database.")
    parser.add_argument("folder", type=str, help="Folder containing the JSONL data files")
    args = parser.parse_args()

    # Connect to SQLite database
    conn = sqlite3.connect(f"{args.folder}/{args.folder}_comments.db")
    cursor = conn.cursor()

    # Create table
    create_table(cursor)

    # Insert data
    insert_data(cursor, f"{args.folder}/{args.folder}_comments.jsonl")

    # Commit and close connection
    conn.commit()
    conn.close()

    print("Data successfully inserted into the database.")

if __name__ == "__main__":
    main()
