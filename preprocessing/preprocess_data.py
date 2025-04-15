import sqlite3
import pandas as pd
import os
import time

from torch.distributed.nn import all_reduce
from tqdm import tqdm
from transformers import pipeline
from transformers import logging

logging.set_verbosity_error()

# Constants for file paths
DB_PATH_COMMENTS = "unpopularopinion/unpopularopinion_toplevel_comments.db"
DB_PATH_SUBMISSIONS = "unpopularopinion/unpopularopinion_submissions.db"
FILE_BOTS = "unpopularopinion/bots.csv"
FILE_ALREADY_PROCESSED = "unpopularopinion/already_processed.csv"
FILE_AUTHORS_100_COMMENTS = "unpopularopinion/authors_100_comments.csv"
FILE_POSTS_SELECTED_USERS = "unpopularopinion/posts_with_at_least_one_comment_from_selected_users.csv"
FILE_AUTHORS_10_NEUTRAL = "unpopularopinion/authors_with_at_least_10_distinct_neutral_post_comments.csv"

MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"
SENTIMENT_PIPELINE = pipeline("sentiment-analysis", model=MODEL_NAME, truncation=True, max_length=512, tokenizer=MODEL_NAME)

def get_db_connection(database: str = "comments") -> sqlite3.Connection:
    if database == "comments":
        return sqlite3.connect(DB_PATH_COMMENTS)
    elif database == "submissions":
        return sqlite3.connect(DB_PATH_SUBMISSIONS)


def parse_csv_to_value_list(csv_path, column_name):
    raw_values = pd.read_csv(csv_path)[column_name]
    return "(" + ", ".join([f"'{value}'" for value in raw_values]) + ")"


def filter_users_with_at_least_1_comment_on_at_least_100_posts(connection) -> pd.DataFrame:
    # return authors who have at least 1 entry (=comment) with at least 100 different parent_ids (=submissions)
    # ->: at least 100 rows with the same author and each row has a different parent_id
    column_name = "author"
    parsed_values = parse_csv_to_value_list(FILE_BOTS, column_name)

    command = ("SELECT author, count(DISTINCT parent_id) as distinct_post_comments FROM comments "
               f"WHERE author NOT IN {parsed_values} "
               "GROUP BY author "
               "HAVING distinct_post_comments >= 100 "
               "ORDER BY distinct_post_comments DESC")

    cursor = connection.cursor()
    cursor.execute(command)
    response = cursor.fetchall()
    cursor.close()

    df = pd.DataFrame(response, columns=["author", "count"])
    df.to_csv(FILE_AUTHORS_100_COMMENTS, index=False)


def filter_posts_with_at_least_one_comment_from_users(connection):
    column_name = "author"
    authors = parse_csv_to_value_list(FILE_AUTHORS_100_COMMENTS, column_name)

    # select the id's of the posts on which at least 1 author with more than 100 distinct comments commented on
    command = ("SELECT DISTINCT parent_id FROM comments "
               f"WHERE author IN {authors}")

    cursor = connection.cursor()
    cursor.execute(command)
    response = cursor.fetchall()
    cursor.close()

    parent_ids = [r[0].replace("t3_", "") for r in response]

    df = pd.DataFrame(parent_ids, columns=["parent_id"])
    df.to_csv(FILE_POSTS_SELECTED_USERS, index=False)


def create_neutral_sentiment_table(connection):
    query = ("CREATE TABLE IF NOT EXISTS neutral_sentiment_posts ("
             "post_id TEXT PRIMARY KEY, "
             "title_score REAL,"
             "body_score REAL,"
             "FOREIGN KEY (post_id) REFERENCES posts(id));")

    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()


def get_posts_to_analyze(connection):
    column_name = "parent_id"
    parsed_values = parse_csv_to_value_list(FILE_POSTS_SELECTED_USERS, column_name)

    command = ("SELECT id, title, selftext, author "
               "FROM posts "
               f"WHERE id IN {parsed_values} ")

    cursor = connection.cursor()
    cursor.execute(command)

    # Get total count for progress bar
    count_cursor = connection.cursor()
    count_cursor.execute(f"SELECT COUNT(*) FROM posts WHERE id IN {parsed_values}")
    total_count = count_cursor.fetchone()[0]
    count_cursor.close()

    return cursor, total_count


def analyse_sentiment(content: list) -> list:
    return SENTIMENT_PIPELINE(content)


def process_posts_sentiment(connection):
    # Create table if it doesn't exist
    create_neutral_sentiment_table(connection)

    # Get cursor with posts and total count
    cursor, total_count = get_posts_to_analyze(connection)

    # Process in batches with progress reporting
    batch_size = 500
    processed_count = 0
    start_time = time.time()
    batch_times = []

    select_cursor = connection.cursor()

    # get list of all ids already processed
    if os.path.exists(FILE_ALREADY_PROCESSED):
        processed_post_ids = set(pd.read_csv(FILE_ALREADY_PROCESSED)["post_id"].to_list())
    else:
        processed_post_ids = set()
    print(f"{len(processed_post_ids)} of {total_count} posts have already been processed.")
    total_count = total_count - len(processed_post_ids)
    print(f"{total_count} posts left to process.")


    # Create progress bar
    pbar = tqdm(total=total_count, desc="Processing posts")

    failed_to_process = []
    count = 0
    while True:
        count += 1
        if count == 5:
            break
        batch_start_time = time.time()
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break

        # remove rows which already have been processed
        rows = [row for row in rows if row[0] not in processed_post_ids]
        if not rows:
            print("Already processed all rows. Continuing to next batch ...")
            continue
        else:
            print(f"Removed {batch_size-len(rows)} rows since they have already been processed.")

        try:
            sentiment_results_title = analyse_sentiment([row[1] for row in rows])
            sentiment_results_body = analyse_sentiment([row[2] for row in rows])
        except Exception as e:
            print(f"Failed processing batch with exception {e}")
            row_ids = [row[0] for row in rows]
            failed_to_process += row_ids
            continue

        neutral_sentiment_posts = []
        post_ids = []
        for row, (title_sentiment_result, body_sentiment_result) in zip(rows, zip(sentiment_results_title, sentiment_results_body)):
            post_id = row[0]
            if title_sentiment_result["label"] == "neutral" or body_sentiment_result["label"] == "neutral":
                title_score = -1
                body_score = -1
                if title_sentiment_result["label"] == "neutral":
                    title_score = round(title_sentiment_result["score"], 2)
                if body_sentiment_result["label"] == "neutral":
                    body_score = round(body_sentiment_result["score"], 2)

                neutral_sentiment_posts.append((post_id, title_score, body_score))
            post_ids.append(post_id)

        if neutral_sentiment_posts:
            select_cursor.executemany(
                f"INSERT OR REPLACE INTO neutral_sentiment_posts (post_id, title_score, body_score) VALUES (?, ?, ?)",
                neutral_sentiment_posts
            )
        connection.commit()

        # add processed rows to file
        df = pd.DataFrame(post_ids, columns=["post_id"])
        df.to_csv(FILE_ALREADY_PROCESSED, index=False, mode="a")
        print(f"Added {len(post_ids)} posts to {FILE_ALREADY_PROCESSED}.")

        # Calculate time for this batch
        batch_end_time = time.time()
        batch_time = batch_end_time - batch_start_time
        batch_times.append(batch_time)

        # Update progress
        processed_count += len(rows)
        pbar.update(len(rows))

        # Calculate progress and timing information
        elapsed_time = time.time() - start_time
        progress_percent = (processed_count / total_count) * 100

        # Calculate estimated time remaining
        avg_time_per_batch = sum(batch_times) / len(batch_times)
        remaining_batches = (total_count - processed_count) / batch_size
        estimated_time_remaining = avg_time_per_batch * remaining_batches

        print(f"Processed {processed_count}/{total_count} posts ({progress_percent:.2f}%). "
              f"Found {len(neutral_sentiment_posts)} neutral sentiment posts. "
              f"Elapsed time: {elapsed_time:.2f}s. "
              f"Estimated time remaining: {estimated_time_remaining:.2f}s "
              f"({estimated_time_remaining / 60:.2f}min)")


    pbar.close()
    cursor.close()
    select_cursor.close()


def create_neutral_sentiment_post_table():
    connection = get_db_connection("submissions")
    process_posts_sentiment(connection)
    connection.close()


def filter_users_with_at_least_one_comment_on_at_least_10_neutral_posts():
    #TODO only keep those who have both psot and body neutral?
    # create sentiment analysis for all posts that have comments of the authors in author list
    create_neutral_sentiment_post_table()

    # for each author, count on how many neutral sentiment post they commented on
    ## for each author, get a list of all post_ids they commented on
    ## count if at least 10 of these are in neutral_sentiment_post table
    ## if yes, keep the author

    df_authors = pd.read_csv(FILE_AUTHORS_100_COMMENTS)
    author_set = set(df_authors["author"])

    # Connect to comments DB and attach submissions DB
    conn = get_db_connection("comments")
    cursor = conn.cursor()

    cursor.execute(f"ATTACH DATABASE '{DB_PATH_SUBMISSIONS}' AS submissions_db")
    cursor.execute("""
        SELECT c.author
        FROM comments c
        JOIN submissions_db.neutral_sentiment_posts n
          ON REPLACE(c.parent_id, 't3_', '') = n.post_id
        GROUP BY c.author
        HAVING COUNT(DISTINCT n.post_id) >= 10
    """)
    result_authors = [row[0] for row in cursor.fetchall()]

    # Filter to original author list
    filtered_authors = [author for author in result_authors if author in author_set]

    percentage = round((len(filtered_authors) / len(author_set)) * 100, 2)
    print(
        f"Found {len(filtered_authors)} users from the original list with ≥10 comments on neutral posts ({percentage}%).")
    print(f"Writing results to {FILE_AUTHORS_10_NEUTRAL} ...")

    pd.DataFrame(filtered_authors, columns=["author"]).to_csv(FILE_AUTHORS_10_NEUTRAL, index=False)


def main():
    if not os.path.isfile(DB_PATH_COMMENTS):
        print(f"Didn't find the toplevel comments db. Please run comments-db.py before running this script. ")
        return

    connection = get_db_connection()
    if not os.path.isfile(FILE_AUTHORS_100_COMMENTS):
        print(f"Didn't find {FILE_AUTHORS_100_COMMENTS}. Creating it...")
        filter_users_with_at_least_1_comment_on_at_least_100_posts(connection)
        print(f"Finished creating {FILE_AUTHORS_100_COMMENTS}")

    if not os.path.isfile(FILE_POSTS_SELECTED_USERS):
        print(f"Didn't find {FILE_POSTS_SELECTED_USERS}. Creating it...")
        filter_posts_with_at_least_one_comment_from_users(connection)
        print(f"Finished creating {FILE_POSTS_SELECTED_USERS}")

    if not os.path.isfile(FILE_AUTHORS_10_NEUTRAL):
        print(f"Didn't find {FILE_AUTHORS_10_NEUTRAL}. Creating it... ")
        filter_users_with_at_least_one_comment_on_at_least_10_neutral_posts()
        print(f"Finished creating {FILE_AUTHORS_10_NEUTRAL}")

    connection.close()


if __name__ == "__main__":
    main()
