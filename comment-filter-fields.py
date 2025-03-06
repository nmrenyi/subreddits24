import pandas as pd
import os
import argparse

def convert_json_to_tsv(input_json, output_tsv):
    # Load the JSON file
    comments_df = pd.read_json(input_json, lines=True)
    
    # Define the necessary fields to keep
    comments_fields_to_keep = [
        "author", "subreddit", "link_id", "parent_id", "score", "ups", "downs",
        "created_utc", "body", "id", "author_flair_text", "controversiality",
        "subreddit_id", "retrieved_on", "edited"
    ]
    
    # Filter the DataFrame
    core_comments_df = comments_df[comments_fields_to_keep]
    
    # Save to TSV format
    core_comments_df.to_csv(output_tsv, sep='\t', index=False)
    print(f"TSV file saved at: {output_tsv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON comments file to TSV with necessary fields.")
    parser.add_argument("input_json", help="Path to the input JSON file")
    parser.add_argument("output_tsv", help="Path to save the output TSV file")
    
    args = parser.parse_args()
    convert_json_to_tsv(args.input_json, args.output_tsv)

