import pandas as pd
from tqdm import tqdm
import sqlite3

def get_comment_record(db_conn, comment_id):
    cursor = db_conn.cursor()
    cursor.execute("SELECT author, parent_id FROM comments WHERE id = ?", (comment_id,))
    result = cursor.fetchone()
    if result:
        return {
            'author': result[0],
            'post_id': result[1],
            'comment_id': comment_id
        }
    return None  # Don't raise an error

def get_post_sentiment(post_conn, post_id):
    cursor = post_conn.cursor()
    cursor.execute(
        "SELECT top_title_sentiment, top_title_score, top_body_sentiment, top_body_score FROM all_sentiment_posts WHERE post_id = ?",
        (post_id[3:] if post_id.startswith('t3_') else post_id,))
    result = cursor.fetchone()
    if result:
        return result[0], result[1], result[2], result[3]
    return None  # Don't raise an error

def get_comment_sentiment(db_conn, comment_id):
    cursor = db_conn.cursor()
    cursor.execute("SELECT top_comment_sentiment, top_comment_score FROM all_sentiment_comments WHERE comment_id = ?", (comment_id,))
    result = cursor.fetchone()
    if result:
        return result[0], result[1]
    return None  # Don't raise an error

def main():
    db_conn = sqlite3.connect('../data/unpopularopinion_toplevel_comments.db')
    post_conn = sqlite3.connect('../data/unpopularopinion_submissions.db')

    try:
        err_cnt = 0
        authors = set(pd.read_csv('../data/authors_with_at_least_10_distinct_neutral_post_comments.csv')['author'].tolist())
        comments_df = pd.read_csv('../data/filtered_comment_sentiments.csv')
        comments_id = comments_df['comment_id'].tolist()

        valid_comments = []
        for comment_id in tqdm(comments_id):
            try:
                comment_record = get_comment_record(db_conn, comment_id)
                if comment_record is None or comment_record['author'] not in authors:
                    # print(f"Skipping comment {comment_id} due to author not in valid authors")
                    continue

                post_sentiment = get_post_sentiment(post_conn, comment_record['post_id'])
                comment_sentiment = get_comment_sentiment(db_conn, comment_id)

                if post_sentiment is None or comment_sentiment is None:
                    print(f"comment {comment_id} not found in sentiment")
                    err_cnt += 1
                    continue

                comment_record['post_title_sentiment'], comment_record['post_title_score'], \
                comment_record['post_body_sentiment'], comment_record['post_body_score'] = post_sentiment

                comment_record['comment_sentiment_type'], comment_record['comment_sentiment_value'] = comment_sentiment

                valid_comments.append(comment_record)

            except Exception as e:
                print(f"Skipping comment {comment_id} due to error: {e}")
                continue
        
        print(f"Total comments: {len(comments_id)}, valid comments: {len(valid_comments)}, error count: {err_cnt}")
        result_df = pd.DataFrame(valid_comments)
        result_df.to_csv('../data/valid_comments.csv', index=False)

    finally:
        db_conn.close()
        post_conn.close()

if __name__ == '__main__':
    main()
