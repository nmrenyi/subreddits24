import sqlite3
import pandas as pd
import re
import tiktoken
from collections import defaultdict


def count_words(text):
    """Count words in a given text."""
    return len(re.findall(r'\w+', text)) if text else 0


def count_tokens(text, encoder):
    """Count tokens in a given text using OpenAI's tokenizer."""
    return len(encoder.encode(text)) if text else 0


def get_yearly_stats(comments_db_path, submissions_db_path):
    """Retrieve yearly statistics from both the comments and submissions databases."""
    conn_comments = sqlite3.connect(comments_db_path)
    conn_submissions = sqlite3.connect(submissions_db_path)
    cursor_comments = conn_comments.cursor()
    cursor_submissions = conn_submissions.cursor()

    # Initialize OpenAI tokenizer (GPT-4 / GPT-3.5 model)
    encoder = tiktoken.encoding_for_model("gpt-4")

    # Dictionary to store aggregated yearly data
    yearly_data = defaultdict(lambda: {
        "year": 0,
        "number_of_posts": 0,
        "number_of_top_level_comments": 0,
        "parent_post_exists": 0,
        "words_posts": 0,
        "words_tl_comments": 0,
        "token_posts": 0,
        "token_tl_comments": 0
    })

    ### Process submissions (posts)
    cursor_submissions.execute("""
        SELECT strftime('%Y', datetime(created_utc, 'unixepoch')) AS year, id, title, selftext 
        FROM posts
        WHERE created_utc >= strftime('%s', '2020-01-01')
    """)

    post_ids_by_year = defaultdict(set)

    for year, post_id, title, selftext in cursor_submissions.fetchall():
        post_ids_by_year[year].add(post_id)
        yearly_data[year]["year"] = int(year)
        yearly_data[year]["number_of_posts"] += 1
        yearly_data[year]["words_posts"] += count_words(title) + count_words(selftext)
        yearly_data[year]["token_posts"] += count_tokens(title, encoder) + count_tokens(selftext, encoder)

    ### Process comments (top-level comments)
    cursor_comments.execute("""
        SELECT strftime('%Y', datetime(created_utc, 'unixepoch')) AS year, parent_id, body 
        FROM comments
        WHERE parent_id LIKE 't3_%' AND created_utc >= strftime('%s', '2020-01-01')
    """)

    for year, parent_id, body in cursor_comments.fetchall():
        parent_post_id = parent_id.replace("t3_", "")
        yearly_data[year]["year"] = int(year)
        yearly_data[year]["number_of_top_level_comments"] += 1
        yearly_data[year]["words_tl_comments"] += count_words(body)
        yearly_data[year]["token_tl_comments"] += count_tokens(body, encoder)

        # Check if the parent post exists in submissions
        if parent_post_id in post_ids_by_year[year]:
            yearly_data[year]["parent_post_exists"] += 1

    conn_comments.close()
    conn_submissions.close()

    # Convert dictionary to pandas DataFrame
    df = pd.DataFrame(list(yearly_data.values()))

    return df


def main():
    folder = "unpopularopinion"
    comments_db_path = f"{folder}/{folder}_comments.db"  # Path to comments database
    submissions_db_path = f"{folder}/{folder}_submissions.db"  # Path to submissions database

    df = get_yearly_stats(comments_db_path, submissions_db_path)
    df.to_csv("llm_cost_estimate.csv")


if __name__ == "__main__":
    main()