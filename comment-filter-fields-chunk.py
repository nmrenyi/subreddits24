import pandas as pd
import os
import argparse
from tqdm import tqdm

def convert_json_to_tsv(input_json, output_tsv, chunk_size=10000):
    # Define the necessary fields to keep
    comments_fields_to_keep = [
        "author", "subreddit", "link_id", "parent_id", "score", "ups", "downs",
        "created_utc", "body", "id", "author_flair_text", "controversiality",
        "subreddit_id", "retrieved_on", "edited"
    ]
    
    # Open the output TSV file and write headers
    with open(output_tsv, 'w', encoding='utf-8') as out_file:
        out_file.write('\t'.join(comments_fields_to_keep) + '\n')
        
        # Estimate total lines for progress bar
        total_lines = sum(1 for _ in open(input_json, 'r', encoding='utf-8'))
        num_chunks = total_lines // chunk_size + 1
        
        # Process the JSON file in chunks using pandas' read_json with chunksize
        try:
            with tqdm(total=num_chunks, desc="Processing", unit="chunk") as pbar:
                for chunk in pd.read_json(input_json, lines=True, chunksize=chunk_size):
                    # Ensure all required fields exist in the chunk, filling missing ones with NaN
                    for field in comments_fields_to_keep:
                        if field not in chunk.columns:
                            chunk[field] = None
                    
                    chunk_filtered = chunk[comments_fields_to_keep]
                    chunk_filtered.to_csv(out_file, sep='\t', index=False, header=False, mode='a', na_rep='NULL')
                    pbar.update(1)
        except Exception as e:
            print(f"Error processing file: {e}")
    
    print(f"TSV file saved at: {output_tsv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON comments file to TSV with necessary fields.")
    parser.add_argument("input_json", help="Path to the input JSON file")
    parser.add_argument("output_tsv", help="Path to save the output TSV file")
    parser.add_argument("--chunk_size", type=int, default=10000, help="Number of lines to process in each chunk")
    
    args = parser.parse_args()
    convert_json_to_tsv(args.input_json, args.output_tsv, args.chunk_size)