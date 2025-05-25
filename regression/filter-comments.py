import pandas as pd
from tqdm import tqdm
import sqlite3

def get_comment_record(db_conn, comment_id):
    # Use the passed database connection to query the record
    # Example (adjust according to your actual database schema):
    cursor = db_conn.cursor()
    cursor.execute("SELECT author, parent_id FROM comments WHERE id = ?", (comment_id,))
    result = cursor.fetchone()
    if result:
        return {
            'author': result[0],
            'post_id': result[1],
            'comment_id': comment_id
        }
    raise ValueError(f"Comment {comment_id} not found")

def main():
    # Create database connection once
    db_conn = sqlite3.connect('../data/unpopularopinion_toplevel_comments.db')  # adjust path as needed
    try:
        authors = set(pd.read_csv('../data/authors_with_at_least_10_distinct_neutral_post_comments.csv')['author'].tolist())
        comments_df = pd.read_csv('../data/filtered_comment_sentiments.csv')
        comments_id = comments_df['comment_id'].tolist()

        valid_comments = []
        for comment_id in tqdm(comments_id):
            comment_record = get_comment_record(db_conn, comment_id)
            if comment_record and comment_record['author'] in authors:
                valid_comments.append(comment_record)

        # Save results if needed
        result_df = pd.DataFrame(valid_comments)
        result_df.to_csv('../data/valid_comments.csv', index=False)

    finally:
        db_conn.close()  # Ensure connection is closed even if an error occurs

if __name__ == '__main__':
    main()  
