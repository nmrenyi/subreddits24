import json
import pandas as pd
import argparse
from collections import Counter
from tqdm import tqdm

def count_posts_by_author(file_path, chunk_size=10000):
    author_counts = Counter()
    
    # Estimate total lines for progress bar
    with open(file_path, 'r', encoding='utf-8') as file:
        total_lines = sum(1 for _ in file)

    # Read file in chunks with pandas
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = pd.read_json(file, lines=True, chunksize=chunk_size)
        
        for chunk in tqdm(reader, total=total_lines//chunk_size + 1, desc="Processing JSONL", unit=" chunks"):
            for _, row in chunk.iterrows():
                author = row.get("author")
                if author and author != "[deleted]":
                    author_counts[author] += 1

    return author_counts

def export_to_tsv(author_counts, output_file):
    df = pd.DataFrame(author_counts.items(), columns=["Author", "Post Count"])
    df.to_csv(output_file, sep='\t', index=False, encoding='utf-8')
    print(f"Exported author post counts to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Count posts per author from a JSONL file and export as TSV.")
    parser.add_argument("folder", type=str, help="Path to the input folder")
    parser.add_argument("--chunk_size", type=int, default=10000, help="Number of lines to process per chunk (default: 10000).")

    args = parser.parse_args()

    # Process the file and export results
    author_counts = count_posts_by_author(f'{args.folder}/{args.folder}_submissions.jsonl', args.chunk_size)
    export_to_tsv(author_counts, f'{args.folder}/{args.folder}_user_post_count.tsv')
