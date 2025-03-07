import json
import pandas as pd
import argparse
from collections import defaultdict
from tqdm import tqdm

def count_comments_and_unique_posts(file_path, chunk_size=10000):
    user_comment_counts = defaultdict(int)
    user_unique_posts = defaultdict(set)

    # Estimate total lines for progress bar
    with open(file_path, 'r', encoding='utf-8') as file:
        total_lines = sum(1 for _ in file)

    # Read file in chunks with pandas
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = pd.read_json(file, lines=True, chunksize=chunk_size)
        
        for chunk in tqdm(reader, total=total_lines//chunk_size + 1, desc="Processing JSONL", unit=" chunks"):
            for _, row in chunk.iterrows():
                author = row.get("author")
                link_id = row.get("link_id")  # Post identifier
                
                if author and author != "[deleted]" and link_id:
                    user_comment_counts[author] += 1
                    user_unique_posts[author].add(link_id)

    # Convert unique post counts from sets to integer counts
    user_data = {user: (count, len(posts)) for user, (count, posts) in zip(user_comment_counts.keys(), zip(user_comment_counts.values(), user_unique_posts.values()))}

    return user_data

def export_to_tsv(user_data, output_file):
    df = pd.DataFrame.from_dict(user_data, orient='index', columns=["Comment Count", "Unique Posts"])
    df.index.name = "Author"
    df.to_csv(output_file, sep='\t', encoding='utf-8')
    print(f"Exported user comment and unique post counts to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Count comments and unique posts per user from a JSONL file and export as TSV.")
    parser.add_argument("folder", type=str, help="Path to the input folder")
    parser.add_argument("--chunk_size", type=int, default=10000, help="Number of lines to process per chunk (default: 10000).")

    args = parser.parse_args()

    # Process the file and export results
    user_data = count_comments_and_unique_posts(f'{args.folder}/{args.folder}_comments.jsonl', args.chunk_size)
    export_to_tsv(user_data, f'{args.folder}/{args.folder}_user_comment_count.tsv')
