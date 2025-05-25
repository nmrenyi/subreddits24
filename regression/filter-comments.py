import pandas as pd
from tqdm import tqdm

def get_comment_record(comment_id):
    pass

def main():
    authors = set(pd.read_csv('../data/authors_with_at_least_10_distinct_neutral_post_comments.csv')['author'].tolist())
    comments_df = pd.read_csv('../data/filtered_comment_sentiments.csv')
    comments_id = comments_df['comment_id'].tolist()

    valid_comments = []
    for comment_id in tqdm(comments_id):
        comment_record = get_comment_record(comment_id)
        author = comment_record['author']
        if author in authors:
            valid_comments.append({
                'comment_id': comment_id,
                'post_id': comment_record['post_id'],
                'author': author,
            })



if __name__ == '__main__':
    main()
