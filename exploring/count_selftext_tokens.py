import sqlite3
import pandas as pd
from transformers import AutoTokenizer
import matplotlib.pyplot as plt

# === Config ===
DB_PATH_SUBMISSIONS = "unpopularopinion/unpopularopinion_submissions.db"

# === Load tokenizer ===
model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(model_name)

# === Connect to database ===
conn = sqlite3.connect(DB_PATH_SUBMISSIONS)

# === Use regular SQL to get N submissions ===
cursor = conn.cursor()
cursor.execute(f"SELECT id, selftext FROM posts")
rows = cursor.fetchall()

# === Also get 10 longest selftext entries ===
cursor.execute("""
    SELECT id, selftext, LENGTH(COALESCE(selftext, '')) AS selftext_length
    FROM posts
    ORDER BY selftext_length DESC
    LIMIT 10
""")
longest_rows = cursor.fetchall()
cursor.close()
conn.close()

# === Load into DataFrames ===
df = pd.DataFrame(rows, columns=["id", "selftext"])
df_longest = pd.DataFrame(longest_rows, columns=["id", "selftext", "selftext_length"])

# === Count tokens ===
def count_tokens(selftext):
    return len(tokenizer.encode(selftext or "", truncation=False))

df["num_tokens"] = df["selftext"].apply(count_tokens)
df_longest["num_tokens"] = df_longest["selftext"].apply(count_tokens)

# === Show overall stats ===
print(df[["id", "num_tokens"]].head())
print(f"\nAverage number of tokens: {df['num_tokens'].mean():.2f}")
print(f"Max number of tokens: {df['num_tokens'].max()}")
print(f"Min number of tokens: {df['num_tokens'].min()}")
too_long_input = len(df.num_tokens[df.num_tokens <= 512]) / len(df.num_tokens)
print(f"{too_long_input*100:.2f}% of the input will not be truncated.")

# === Plot ===
plt.hist(df["num_tokens"], bins=100)
plt.axvline(512, color="r", label="512-token limit")
plt.title("Token Count Distribution of Submissions (selftext only)")
plt.xlabel("Number of Tokens")
plt.ylabel("Number of Posts")
plt.legend()
plt.tight_layout()
plt.show()

# === Show 10 longest selftexts with token counts ===
print("\nTop 10 longest `selftext` entries by character count:")
print(df_longest[["id", "selftext_length", "num_tokens"]])
df_longest.to_csv("longest_selftext.csv")