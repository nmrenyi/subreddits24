import pandas as pd
import sqlite3
import seaborn as sns
import matplotlib.pyplot as plt

# File paths – update if needed
DB_PATH_COMMENTS = "unpopularopinion/unpopularopinion_toplevel_comments.db"
DB_PATH_SUBMISSIONS = "unpopularopinion/unpopularopinion_submissions.db"
FILE_AUTHORS_100_COMMENTS = "unpopularopinion/authors_100_comments.csv"
FILE_AUTHORS_10_NEUTRAL = "unpopularopinion/authors_with_at_least_10_distinct_neutral_post_comments.csv"

# Load comment database
conn_comments = sqlite3.connect(DB_PATH_COMMENTS)
all_users = pd.read_sql_query("SELECT DISTINCT author FROM comments", conn_comments)
total_users = len(all_users)

# Same as total unique users
users_commented_once = total_users

# Users with ≥100 distinct commented posts
users_100_df = pd.read_csv(FILE_AUTHORS_100_COMMENTS)
users_100 = len(users_100_df)

# Users with ≥10 comments on neutral posts
users_neutral_df = pd.read_csv(FILE_AUTHORS_10_NEUTRAL)
users_neutral = len(users_neutral_df)

# Load sentiment info from submission DB
conn_submissions = sqlite3.connect(DB_PATH_SUBMISSIONS)
neutral_df = pd.read_sql_query("SELECT * FROM neutral_sentiment_posts", conn_submissions)
title_neutral = (neutral_df['title_score'] != -1).sum()
body_neutral = (neutral_df['body_score'] != -1).sum()
both_neutral = ((neutral_df['title_score'] != -1) & (neutral_df['body_score'] != -1)).sum()
total_posts = pd.read_sql_query("SELECT COUNT(*) AS count FROM posts", conn_submissions)["count"].iloc[0]

# Close connections
conn_comments.close()
conn_submissions.close()

# DataFrames for plotting
user_stats = pd.DataFrame({
    "Category": [
        "All users",
        "Commented ≥1 time",
        "Commented on ≥100 posts",
        "Commented on ≥10 neutral posts"
    ],
    "Count": [total_users, users_commented_once, users_100, users_neutral]
})
user_stats["Percent"] = (user_stats["Count"] / total_users * 100).round(2)

neutral_stats = pd.DataFrame({
    "Category": ["Title neutral", "Body neutral", "Both neutral"],
    "Count": [title_neutral, body_neutral, both_neutral]
})
neutral_stats["Percent"] = (neutral_stats["Count"] / total_posts * 100).round(2)

# Set seaborn style
sns.set_theme(style="whitegrid", palette="pastel")

# Plot: User Statistics
plt.figure(figsize=(10, 6))
sns.barplot(x="Percent", y="Category", data=user_stats)
plt.title("User Statistics")
for i, (count, percent) in enumerate(zip(user_stats["Count"], user_stats["Percent"])):
    plt.text(percent + 0.5, i, f"{percent}% ({count})", va="center")
plt.xlim(0, 110)
plt.tight_layout()
plt.show()

# Plot: Neutral Sentiment Post Statistics
plt.figure(figsize=(10, 4))
sns.barplot(x="Percent", y="Category", data=neutral_stats)
plt.title("Neutral Sentiment Post Statistics")
for i, (count, percent) in enumerate(zip(neutral_stats["Count"], neutral_stats["Percent"])):
    plt.text(percent + 0.5, i, f"{percent}% ({count})", va="center")
plt.xlim(0, 110)
plt.tight_layout()
plt.show()