import pandas as pd
import os
import argparse
from tqdm import tqdm

def convert_json_to_jsonl(input_json, output_jsonl, chunk_size=10000):
    # Define the necessary fields to keep for submissions
    submission_fields_to_keep = [
        "id", "subreddit", "subreddit_id", "title", "selftext", "url", "permalink",
        "created_utc", "score", "num_comments", "ups", "downs", "author",
        "author_flair_text", "is_self", "domain", "over_18", "media", "edited",
        "stickied", "distinguished"
    ]
    
    # Estimate total lines for progress bar
    total_lines = sum(1 for _ in open(input_json, 'r', encoding='utf-8'))
    num_chunks = total_lines // chunk_size + 1
    print(f"Total lines: {total_lines}, processing in chunks of {chunk_size} lines")
    
    # Process the JSON file in chunks using pandas' read_json with chunksize
    try:
        with tqdm(total=num_chunks, desc="Processing", unit="chunk") as pbar, open(output_jsonl, 'w', encoding='utf-8') as out_file:
            for chunk in pd.read_json(input_json, lines=True, chunksize=chunk_size):
                # Ensure all required fields exist in the chunk, filling missing ones with None
                for field in submission_fields_to_keep:
                    if field not in chunk.columns:
                        chunk[field] = None
                
                # Filter the chunk to keep only the necessary fields
                chunk_filtered = chunk[submission_fields_to_keep]
                
                # Write to JSONL file (each row as a separate JSON object)
                chunk_filtered.to_json(out_file, orient='records', lines=True, force_ascii=False)
                
                pbar.update(1)
    except Exception as e:
        print(f"Error processing file: {e}")
    
    print(f"JSONL file saved at: {output_jsonl}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON submissions file to JSONL with necessary fields.")
    parser.add_argument("input_json", help="Path to the input JSON file")
    parser.add_argument("output_jsonl", help="Path to save the output JSONL file")
    parser.add_argument("--chunk_size", type=int, default=10000, help="Number of lines to process in each chunk")
    
    args = parser.parse_args()
    convert_json_to_jsonl(args.input_json, args.output_jsonl, args.chunk_size)