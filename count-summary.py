import argparse
import pandas as pd

def main(folder):
    comments_count = pd.read_csv(f"{folder}/{folder}_user_comment_count.tsv", sep="\t")
    submissions_count = pd.read_csv(f"{folder}/{folder}_user_post_count.tsv", sep="\t")
    # combine based on the Author column
    combined = pd.merge(submissions_count, comments_count, on="Author", how="outer")
    # fill NaN values with 0
    combined.fillna(0, inplace=True)

    # Convert numerical columns to integer type
    numeric_columns = ['Post Count', 'Comment Count', 'Unique Posts']
    for col in numeric_columns:
        if col in combined.columns:
            combined[col] = combined[col].astype(int)

    # rename columns to #posts, #comments, #comments_on_unique_posts
    combined.rename(columns={"Post Count": "#posts", "Comment Count": "#comments", "Unique Posts": "#comments_on_unique_posts"}, inplace=True)

    # adjust the order of the columns to #comments, #comments_on_unique_posts, #posts
    combined = combined[['Author', '#comments', '#comments_on_unique_posts', '#posts']]

    # export to TSV
    combined.sort_values('#comments_on_unique_posts', ascending=False).to_csv(f"{folder}/{folder}_user_summary_lean.tsv", sep="\t", index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Count posts per author from a JSONL file and export as TSV.")
    parser.add_argument("folder", type=str, help="Path to the input folder")
    args = parser.parse_args()

    main(args.folder)
