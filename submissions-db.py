import sqlite3
import json
import argparse
from tqdm import tqdm

def create_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            author TEXT
        )
    """)

def insert_data(cursor, jsonl_file):
    with open(jsonl_file, "r", encoding="utf-8") as file:
        lines = file.readlines()
        for line in tqdm(lines, desc="Inserting posts"):
            post = json.loads(line.strip())
            cursor.execute(
                "INSERT OR IGNORE INTO posts (id, author) VALUES (?, ?)",
                (
                    post.get("id", "unknown"),
                    post.get("author", "unknown")
                )
            )

def main():
    parser = argparse.ArgumentParser(description="Insert JSONL data into a SQLite database.")
    parser.add_argument("folder", type=str, help="Folder containing the JSONL data files")
    args = parser.parse_args()

    conn = sqlite3.connect(f"{args.folder}/{args.folder}_submissions.db")
    cursor = conn.cursor()

    create_table(cursor)
    insert_data(cursor, f"{args.folder}/{args.folder}_submissions.jsonl")

    conn.commit()
    conn.close()

    print("Data successfully inserted into the database.")

if __name__ == "__main__":
    main()
