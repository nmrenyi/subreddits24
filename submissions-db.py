import sqlite3
import json
import argparse
from tqdm import tqdm

def create_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            subreddit TEXT,
            subreddit_id TEXT,
            title TEXT,
            selftext TEXT,
            url TEXT,
            permalink TEXT,
            created_utc INTEGER,
            score INTEGER,
            num_comments INTEGER,
            ups REAL,
            downs REAL,
            author TEXT,
            author_flair_text TEXT,
            is_self INTEGER,
            domain TEXT,
            over_18 INTEGER,
            media TEXT,
            edited INTEGER,
            stickied INTEGER,
            distinguished TEXT
        )
    """)

def insert_data(cursor, jsonl_file):
    with open(jsonl_file, "r", encoding="utf-8") as file:
        lines = file.readlines()
        for line in tqdm(lines, desc="Inserting posts"):
            post = json.loads(line.strip())
            cursor.execute("""
                INSERT OR IGNORE INTO posts (
                    id, subreddit, subreddit_id, title, selftext, url, permalink,
                    created_utc, score, num_comments, ups, downs, author, author_flair_text,
                    is_self, domain, over_18, media, edited, stickied, distinguished
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                post.get("id", "unknown"),
                post.get("subreddit", "unknown"),
                post.get("subreddit_id", "unknown"),
                post.get("title", "unknown"),
                post.get("selftext", ""),
                post.get("url", "unknown"),
                post.get("permalink", "unknown"),
                post.get("created_utc", -1),
                post.get("score", -1),
                post.get("num_comments", -1),
                post.get("ups", -1.0),
                post.get("downs", -1.0),
                post.get("author", "unknown"),
                post.get("author_flair_text", "unknown"),
                int(post.get("is_self", False)),
                post.get("domain", "unknown"),
                int(post.get("over_18", False)),
                json.dumps(post.get("media")) if post.get("media") else "unknown",
                int(post.get("edited", -1)),
                int(post["stickied"]) if post.get("stickied") is not None else -1,
                post.get("distinguished", "unknown")
            ))

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
