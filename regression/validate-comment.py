import pandas as pd
from tqdm import tqdm
import sqlite3

def check_comment_valid(comment_id):
    # check if the commment is valid from the database /Users/renyi/Downloads/food/subreddits24/data/unpopularopinion_toplevel_comments.db
    db_conn = sqlite3.connect('/Users/renyi/Downloads/food/subreddits24/data/unpopularopinion_toplevel_comments.db')
    cursor = db_conn.cursor()
    cursor.execute("SELECT * FROM all_sentiment_comments WHERE comment_id = ?", (comment_id,))
    result = cursor.fetchone()
    if result:
        return True
    return False


def main():
    comments_df = pd.read_csv('../data/valid_comments.csv')
    comments_id = comments_df['comment_id'].tolist()
    for comment_id in tqdm(comments_id):
        valid = check_comment_valid(comment_id)
        if not valid:
            print(f"Comment {comment_id} is not valid")

if __name__ == '__main__':
    main()
