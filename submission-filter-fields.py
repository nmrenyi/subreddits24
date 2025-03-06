import pandas as pd
import os
import argparse

def convert_json_to_tsv(input_json, output_tsv):
    # Load the JSON file
    submission_df = pd.read_json(input_json, lines=True)
    
    # Define the necessary fields to keep
    submission_fields_to_keep = [
        "id", "subreddit", "subreddit_id", "title", "selftext", "url", "permalink",
        "created_utc", "score", "num_comments", "ups", "downs", "author",
        "author_flair_text", "is_self", "domain", "over_18", "media", "edited",
        "stickied", "distinguished"
    ]
    
    # Filter the DataFrame
    core_submission_df = submission_df[submission_fields_to_keep]
    
    # Save to TSV format
    core_submission_df.to_csv(output_tsv, sep='\t', index=False)
    print(f"TSV file saved at: {output_tsv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON submissions file to TSV with necessary fields.")
    parser.add_argument("input_json", help="Path to the input JSON file")
    parser.add_argument("output_tsv", help="Path to save the output TSV file")
    
    args = parser.parse_args()
    convert_json_to_tsv(args.input_json, args.output_tsv)
