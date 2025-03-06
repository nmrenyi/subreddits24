import pandas as pd
import os
import argparse
from tqdm import tqdm

def convert_json_to_tsv(input_json, output_tsv, chunk_size=10000):
    # Define the necessary fields to keep for submissions
    submission_fields_to_keep = [
        "id", "subreddit", "subreddit_id", "title", "selftext", "url", "permalink",
        "created_utc", "score", "num_comments", "ups", "downs", "author",
        "author_flair_text", "is_self", "domain", "over_18", "media", "edited",
        "stickied", "distinguished"
    ]
    
    # Open the output TSV file and write headers
    with open(output_tsv, 'w', encoding='utf-8') as out_file:
        out_file.write('\t'.join(submission_fields_to_keep) + '\n')
        
        # Estimate total lines for progress bar
        total_lines = sum(1 for _ in open(input_json, 'r', encoding='utf-8'))
        num_chunks = total_lines // chunk_size + 1
        print(f"Total lines: {total_lines}, processing in chunks of {chunk_size} lines")
        
        # Process the JSON file in chunks using pandas' read_json with chunksize
        try:
            with tqdm(total=num_chunks, desc="Processing", unit="chunk") as pbar:
                for chunk in pd.read_json(input_json, lines=True, chunksize=chunk_size):
                    # Ensure all required fields exist in the chunk, filling missing ones with NaN
                    for field in submission_fields_to_keep:
                        if field not in chunk.columns:
                            chunk[field] = None
                    
                    chunk_filtered = chunk[submission_fields_to_keep]
                    chunk_filtered.to_csv(out_file, sep='\t', index=False, header=False, mode='a', na_rep='NULL')
                    pbar.update(1)
        except Exception as e:
            print(f"Error processing file: {e}")
    
    print(f"TSV file saved at: {output_tsv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON submissions file to TSV with necessary fields.")
    parser.add_argument("input_json", help="Path to the input JSON file")
    parser.add_argument("output_tsv", help="Path to save the output TSV file")
    parser.add_argument("--chunk_size", type=int, default=10000, help="Number of lines to process in each chunk")
    
    args = parser.parse_args()
    convert_json_to_tsv(args.input_json, args.output_tsv, args.chunk_size)
