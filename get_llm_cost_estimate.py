import pandas as pd


# read in comments
df_comments = pd.read_json("unpopularopinion/unpopularopinion_comments.10000.jsonl", lines=True)
original_df_number_of_rows = len(df_comments)

# only keep top level comments (that is, comments whhose parent_id starts with t3_)
df_comments = df_comments[df_comments["parent_id"].str.startswith("t3_")]
print(f"Number of comments: {len(df_comments)}, {round(len(df_comments) / original_df_number_of_rows, 2) *100}% of the original data")

parent_post_ids = df_comments["parent_id"].tolist()
parent_post_ids = [p_id.replace("t3_", "") for p_id in parent_post_ids]

# read in submissions
df_submissions = pd.read_json("unpopularopinion/unpopularopinion_submissions.10000.jsonl", lines=True)
post_ids = df_submissions["id"].unique().tolist()

# count how many of the kept comments have their parent_id in df_submission (give percentage)
parent_post_exists =[1 for p_id in parent_post_ids if p_id in post_ids]
print(f"{sum(parent_post_exists)} out of {len(parent_post_ids)} comments' parent post exists in the dataset ({sum(parent_post_exists)/len(parent_post_exists)*100:.2f}%)")

ar