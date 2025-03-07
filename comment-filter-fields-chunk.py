import pandas as pd
import os
import argparse
from tqdm import tqdm

def convert_json_to_jsonl(input_json, output_jsonl, chunk_size=10000):
    # Define the necessary fields to keep
    comments_fields_to_keep = [
        "author", "subreddit", "link_id", "parent_id", "score", "ups", "downs",
        "created_utc", "body", "id", "author_flair_text", "controversiality",
        "subreddit_id", "retrieved_on", "edited"
    ]
    
    # Estimate total lines for progress bar
    total_lines = sum(1 for _ in open(input_json, 'r', encoding='utf-8'))
    num_chunks = total_lines // chunk_size + 1
    print(f"Total lines: {total_lines}, processing in chunks of {chunk_size} lines")
    
    # Process the JSON file in chunks using pandas' read_json with chunksize
    try:
        with open(output_jsonl, 'w', encoding='utf-8') as out_file:
            with tqdm(total=num_chunks, desc="Processing", unit="chunk") as pbar:
                for chunk in pd.read_json(input_json, lines=True, chunksize=chunk_size):
                    # Ensure all required fields exist in the chunk, filling missing ones with NaN
                    for field in comments_fields_to_keep:
                        if field not in chunk.columns:
                            chunk[field] = None
                    
                    chunk_filtered = chunk[comments_fields_to_keep]
                    chunk_filtered.to_json(out_file, orient='records', lines=True, force_ascii=False)
                    pbar.update(1)
    except Exception as e:
        print(f"Error processing file: {e}")
    
    print(f"JSONL file saved at: {output_jsonl}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON comments file to JSONL with necessary fields.")
    parser.add_argument('folder', type=str, help='Path to the input folder')
    parser.add_argument("--chunk_size", type=int, default=10000, help="Number of lines to process in each chunk")
    
    args = parser.parse_args()
    convert_json_to_jsonl(f'{args.folder}/{args.folder}_comments', f'{args.folder}/{args.folder}_comments.jsonl', args.chunk_size)
