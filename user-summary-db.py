import pandas as pd
import argparse
import sqlite3
from tqdm import tqdm

def main():
    parser = argparse.ArgumentParser(description="Generate user summary from Reddit data stored in SQLite databases.")
    parser.add_argument('folder', type=str, help="Folder containing SQLite databases")
    args = parser.parse_args()

    folder = args.folder

    # Connect to SQLite databases
    comments_conn = sqlite3.connect(f'{folder}/{folder}_comments.db')
    submissions_conn = sqlite3.connect(f'{folder}/{folder}_submissions.db')

    comments_cursor = comments_conn.cursor()
    submissions_cursor = submissions_conn.cursor()

    # Extract unique users from both databases
    comments_cursor.execute("SELECT DISTINCT author FROM comments")
    submissions_cursor.execute("SELECT DISTINCT author FROM posts")

    users_set = set(user[0] for user in comments_cursor.fetchall())
    users_set.update(user[0] for user in submissions_cursor.fetchall())

    # Get total counts
    comments_cursor.execute("SELECT COUNT(*) FROM comments")
    num_comments_total = comments_cursor.fetchone()[0]

    submissions_cursor.execute("SELECT COUNT(*) FROM posts")
    num_submissions_total = submissions_cursor.fetchone()[0]

    # Initialize list to store user summaries
    user_summaries = []

    # Iterate through users with progress bar
    for user in tqdm(users_set, desc="Processing Users"):
        comments_cursor.execute("SELECT COUNT(*), COUNT(DISTINCT link_id) FROM comments WHERE author = ?", (user,))
        num_comments, num_comments_unique_posts = comments_cursor.fetchone()

        submissions_cursor.execute("SELECT COUNT(*), COUNT(DISTINCT id) FROM posts WHERE author = ?", (user,))
        num_posts, num_posts_unique = submissions_cursor.fetchone()

        # Append user summary to list
        user_summaries.append({
            'user': user,
            'num_comments': num_comments,
            'num_comments_unique_posts': num_comments_unique_posts,
            'num_posts': num_posts,
            'num_posts_unique': num_posts_unique
        })

    # Close database connections
    comments_conn.close()
    submissions_conn.close()

    # Convert list to DataFrame
    user_summary_df = pd.DataFrame(user_summaries)

    # Include dataset sizes in the output file name
    output_file = f'{folder}/user_summary_{num_comments_total}comments_{num_submissions_total}posts.tsv'
    user_summary_df.to_csv(output_file, sep='\t', index=False)

    print(f"User summary saved to {output_file}")

if __name__ == "__main__":
    main()
