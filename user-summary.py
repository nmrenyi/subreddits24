import pandas as pd
import argparse
from tqdm import tqdm  # Import tqdm for progress bar

# Set up argument parser
parser = argparse.ArgumentParser(description="Process user comments and submissions data.")
parser.add_argument("folder", type=str, help="Folder containing the JSONL data files")

# Parse arguments
args = parser.parse_args()
folder = args.folder

# Load data
comments_file = f"{folder}/{folder}_comments.jsonl"
submissions_file = f"{folder}/{folder}_submissions.jsonl"

comments_df = pd.read_json(comments_file, lines=True)
submission_df = pd.read_json(submissions_file, lines=True)

# Extract unique users
users_set = set()
users_set.update(submission_df['author'].dropna())
users_set.update(comments_df['author'].dropna())

# Get dataset sizes
num_comments_total = len(comments_df)
num_submissions_total = len(submission_df)

# Initialize list to store user summaries
user_summaries = []

# Iterate through users with progress bar
for user in tqdm(users_set, desc="Processing Users"):
    user_comments = comments_df[comments_df['author'] == user]
    num_comments = user_comments.shape[0]
    num_comments_unique_posts = user_comments['link_id'].nunique()

    user_posts = submission_df[submission_df['author'] == user]
    num_posts = user_posts.shape[0]
    num_posts_unique = user_posts['id'].nunique()

    # Append user summary to list
    user_summaries.append({
        'user': user,
        'num_comments': num_comments,
        'num_comments_unique_posts': num_comments_unique_posts,
        'num_posts': num_posts,
        'num_posts_unique': num_posts_unique
    })

# Convert list to DataFrame
user_summary_df = pd.DataFrame(user_summaries)

# Generate output file name with dataset sizes
output_file = f"{folder}/user_summary_{num_comments_total}comments_{num_submissions_total}posts_{len(users_set)}users.tsv"

# Export to TSV
user_summary_df.to_csv(output_file, sep='\t', index=False)

print(f"User summary saved to {output_file}")
