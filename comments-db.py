import sqlite3
import json
import argparse
from tqdm import tqdm

KEEP_ONLY_ROOT_COMMENTS = True

def create_table(cursor):
    """Creates the comments table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id TEXT PRIMARY KEY,
            author TEXT,
            subreddit TEXT,
            link_id TEXT,
            parent_id TEXT,
            score INTEGER,
            ups REAL,
            downs REAL,
            created_utc INTEGER,
            body TEXT,
            author_flair_text TEXT,
            controversiality INTEGER,
            subreddit_id TEXT,
            retrieved_on REAL,
            edited INTEGER
        )
    """)

def count_lines(filename):
    with open(filename, 'rb') as f:
        return sum(1 for _ in f)

import json
import logging
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def insert_data(cursor, jsonl_file):
    total_lines = count_lines(jsonl_file)
    inserted_count = 0

    with open(jsonl_file, "r", encoding="utf-8") as file:
        for line in tqdm(file, desc="Inserting data", unit="entry", total=total_lines):
            comment = json.loads(line.strip())
            parent_id = comment.get("parent_id", "")
            link_id = comment.get("link_id", "")

            if parent_id == link_id:
                inserted_count += 1
                cursor.execute("""
                    INSERT OR IGNORE INTO comments 
                    (id, author, subreddit, link_id, parent_id, score, ups, downs, created_utc, body, author_flair_text, 
                     controversiality, subreddit_id, retrieved_on, edited)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    comment.get("id"),
                    comment.get("author", "unknown"),
                    comment.get("subreddit", "unknown"),
                    link_id,
                    parent_id,
                    comment.get("score", -1),
                    comment.get("ups", -1.0),
                    comment.get("downs", -1.0),
                    comment.get("created_utc", -1),
                    comment.get("body", ""),
                    comment.get("author_flair_text", ""),
                    comment.get("controversiality", -1),
                    comment.get("subreddit_id", ""),
                    comment.get("retrieved_on", -1.0),
                    comment.get("edited") if isinstance(comment.get("edited"), int) else -1
                ))

    logging.info(f"Total lines processed: {total_lines}")
    logging.info(f"Lines inserted (matched condition): {inserted_count}")

def main():
    parser = argparse.ArgumentParser(description="Insert JSONL data into a SQLite database.")
    parser.add_argument("folder", type=str, help="Folder containing the JSONL data files")
    args = parser.parse_args()

    # Connect to SQLite database
    conn = sqlite3.connect(f"{args.folder}/{args.folder}_toplevel_comments.db")
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
