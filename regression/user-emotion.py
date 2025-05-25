import sqlite3
from pathlib import Path

import pandas as pd
from tqdm import tqdm


DATA_DIR = Path("../data")
DB_PATH = Path(
    "/Users/renyi/Downloads/food/subreddits24/data/unpopularopinion_submissions.db"
)
COMMENTS_CSV = DATA_DIR / "valid_comments.csv"


def build_user_post_comment_map(df: pd.DataFrame) -> dict[str, list[tuple[str, str]]]:
    """
    Build a mapping of author → list[(post_id, comment_id)].
    """
    return (
        df.groupby("author")[["post_id", "comment_id"]]
        .apply(lambda g: list(g.itertuples(index=False, name=None)))
        .to_dict()
    )


def main() -> None:
    # 1️⃣  Load comments.
    valid_comments = pd.read_csv(COMMENTS_CSV)

    # 2️⃣  Create the user → (post_id, comment_id) mapping.
    user_post_comment_map = build_user_post_comment_map(valid_comments)
    print("finished building user_post_comment_map")


    user_comments_type_on_neutral_posts = { u: [] for u in user_post_comment_map }
    user_comments_value_on_neutral_posts = { u: [] for u in user_post_comment_map }

    post_not_found = 0
    # 3️⃣  Open the DB once and reuse the cursor.
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        for user in tqdm(user_post_comment_map, desc="Users"):
            for post_id, comment_id in user_post_comment_map[user]:
                cursor.execute(
                    """
                    SELECT top_title_sentiment, top_body_sentiment
                    FROM all_sentiment_posts
                    WHERE post_id = ?
                    """,
                    (post_id[3:],),
                )
                row = cursor.fetchone()
                title_sent, body_sent = row if row else (None, None)

                if title_sent == 'neutral' and body_sent == 'neutral':
                    comment_type, comment_value = get_comment_sentiment(comment_id)
                    user_comments_type_on_neutral_posts[user].append(comment_type)
                    user_comments_value_on_neutral_posts[user].append(comment_value)
                elif title_sent is None and body_sent is None:
                    # print(f"Post {post_id} has no sentiment")
                    post_not_found += 1

    print(f"post_not_found: {post_not_found}, {post_not_found / len(valid_comments['post_id'].unique())}")
    # 4️⃣  Save the results.
    pd.DataFrame(
        [
            {'user': user, 'comment_type': ' '.join(user_comments_type_on_neutral_posts[user]), 'comment_value': ' '.join(user_comments_value_on_neutral_posts[user])}
            for user in user_comments_type_on_neutral_posts
        ]
    ).to_csv(
        DATA_DIR / "user_comments_on_neutral_posts.csv",
        index=False,
    )
    print("finished saving user_comments_on_neutral_posts")



if __name__ == "__main__":
    main()