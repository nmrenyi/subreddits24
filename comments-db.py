import sqlite3
import json
import argparse
from tqdm import tqdm

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

def insert_data(cursor, jsonl_file):
    """Reads and inserts data from the JSONL file into the database."""
    with open(jsonl_file, "r", encoding="utf-8") as file:
        lines = file.readlines()

    for line in tqdm(lines, desc="Inserting data", unit="entry"):
        comment = json.loads(line.strip())
        # Use -1 for missing numerical values and empty string for text fields
        cursor.execute("""
            INSERT OR IGNORE INTO comments 
            (id, author, subreddit, link_id, parent_id, score, ups, downs, created_utc, body, author_flair_text, 
             controversiality, subreddit_id, retrieved_on, edited)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            comment.get("id"),
            comment.get("author", "unknown"),  # Default author to "unknown" if missing
            comment.get("subreddit", "unknown"),
            comment.get("link_id", "unknown"),
            comment.get("parent_id", "unknown"),
            comment.get("score", -1),  # Default to -1 if missing
            comment.get("ups", -1.0),  # Default to -1.0 if missing
            comment.get("downs", -1.0),
            comment.get("created_utc", -1),
            comment.get("body", ""),  # Default to empty string if missing
            comment.get("author_flair_text", ""),  # Default to empty string if missing
            comment.get("controversiality", -1),
            comment.get("subreddit_id", ""),
            comment.get("retrieved_on", -1.0),
            comment.get("edited") if isinstance(comment.get("edited"), int) else -1  # Default to -1 if missing
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
