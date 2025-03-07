# Subreddit Processing
This repository holds the code to process [a subreddit dataset](https://www.reddit.com/r/pushshift/comments/1itme1k/separate_dump_files_for_the_top_40k_subreddits/).

## Quick start

There could be a lot of files and scripts in the repository, but you only need to look into several files to get to know the data.

1. [unpopularopinion_comments.10000.jsonl](unpopularopinion/unpopularopinion_comments.10000.jsonl). It contains first 10,000 lines of the cleaned comments file of unpopularopinion subreddit. The unsampled cleaned comments file (47,519,950 lines) could be found in [Google Drive](https://drive.google.com/file/d/1tNkd2uLZaztnPTpINwTChJkEqV0ndh1D/view?usp=share_link).
2. [unpopularopinion_submissions.10000.jsonl](unpopularopinion/unpopularopinion_submissions.10000.jsonl). It contains first 10,000 lines of the cleaned submissions file of unpopularopinion subreddit. The unsampled cleaned submissions file (2,394,871 lines) could be found in [Google Drive](https://drive.google.com/file/d/1xupWizenf6_djj4od4CV_tETncHACgDT/view?usp=share_link).
3. [unpopular_user_summary.tsv](unpopularopinion/unpopularopinion_user_summary.tsv). Per user based #post, #comments, #comments_on_unique_posts.

You can find similar, but more comprehensive file structure in [chinesefood](chinesefood/), because the subreddit of chinesefood is much smaller and the data files could fit better on GitHub.

Here're the [explanations on the keys of the comments file](https://chatgpt.com/share/67cb0d96-9478-8010-8e51-a7e4f24c903c) and the [explanations of the submissions file](https://chatgpt.com/share/67cb0e1c-974c-8010-be11-305cf7a3cad6), by ChatGPT :)


## Details of data processing

### File structure
Here's the full file structure on my computer. The GitHub version contains all the Python scripts, but not all the data files, due to size limit.
```
.
├── chinesefood
│   ├── chinesefood_comments                 # raw comments data
│   ├── chinesefood_submissions              # raw submission data
│   ├── chinesefood_comments.jsonl           # processed comments (json line)
│   ├── chinesefood_submissions.jsonl        # processed submissions (json line)
│   ├── chinesefood_comments.db              # processed comments in database
│   ├── chinesefood_submissions.db           # processed submissions in database
│   ├── chinesefood_user_comment_count.tsv   # #comments and #comments_on_unique_posts per user
│   ├── chinesefood_user_post_count.tsv      # #posts per user
│   └── chinesefood_user_summary.tsv         # Summary of user activity (comments & posts)
├── unpopularopinion
│   ├── unpopularopinion_comments             # raw comments data
│   ├── unpopularopinion_submissions          # raw submission data
│   ├── unpopularopinion_comments.jsonl       # processed comments (json line)
│   ├── unpopularopinion_submissions.jsonl    # processed submissions (json line)
│   ├── unpopularopinion_comments.db          # processed comments in database
│   ├── unpopularopinion_submissions.db       # processed submissions in database
│   ├── unpopularopinion_user_comment_count.tsv  # #comments and #comments_on_unique_posts per user
│   ├── unpopularopinion_user_post_count.tsv     # #posts per user
│   ├── unpopularopinion_user_summary.tsv        # Summary of user activity (comments & posts)
│   ├── unpopularopinion_comments.10000.jsonl    # Sample of 10,000 comments
│   └── unpopularopinion_submissions.10000.jsonl # Sample of 10,000 submissions
├── comment-filter-fields-chunk.py           # Script for filtering necessary fields in comments (chunked processing)
├── comment-filter-fields.py                 # Script for filtering necessary fields in comments (full processing)
├── comments-db.py                           # Script to store comments in a database for efficient querying
├── submission-filter-chunk.py               # Script for filtering necessary fields in submissions (chunked processing)
├── submission-filter-fields.py              # Script for filtering necessary fields in submissions (full processing)
├── submissions-db.py                        # Script to store submissions in a database for efficient querying
├── count-comment.py                         # Script to count comments for each user
├── count-submission.py                      # Script to count posts for each user
├── count-summary.py                         # Script to generate a user activity summary from counts
├── user-summary-db.py                      # Script to calculate user activity summary from the database (could be very slow)
├── user-summary.py                         # Script to generate user activity summary from json line files (could face memory capability problems)
├── reddit-1614740ac8c94505e4ecb9d88be8bed7b6afddd4.torrent  # Torrent file for downloading Reddit dataset
└── readme.md
```

### Data Processing

#### Download raw data
The subreddit dataset is from https://www.reddit.com/r/pushshift/comments/1itme1k/separate_dump_files_for_the_top_40k_subreddits/. You can find the torrent in this file  `./reddit-1614740ac8c94505e4ecb9d88be8bed7b6afddd4.torrent` in the repository. A torrent downloader is needed to download the files from the torrent.

We can get the `<theme>_comments.zst` and `<theme>_submissions.zst` after downloading the selected `<theme>` from the torrent. Decompress the `.zst` files, and we can get `<theme>_comments` (the comments on the subreddit posts) and `<theme>_submissions` (the subreddit posts). Both the comments and the submissions files are consist of json lines, with each line in a file representing a json, i.e., one commment or one post, respectively.


#### Clean raw data
There are several challenges in processing the comments and the submissions files.

1. Size. `unpopularopinion_comments` takes up 64G, while `unpopular_submissions` takes up 5.4G.
2. Not uniformed format. The json lines in one comments file, generally share the same keys, as is [verified by ChatGPT by comparing the keys of several lines](https://chatgpt.com/share/67cafa93-d3ac-8010-8d31-4d6b9dfa7867). However, it's not the case in submission files, where [the json lines don't share the same keys](https://chatgpt.com/share/67cafb02-8f34-8010-8a97-44edb48a7e79).
3. Redundant keys. There are too many keys to handle in the raw comments and submissions files, which could be around 100 keys in the json file. These keys are not all necessary for our project. We used ChatGPT to choose the necessary fields from the randomly selected lines of [comments file](https://chatgpt.com/share/67cafccc-c528-8010-aa3e-d50cdfdfe331) and [submissions file](https://chatgpt.com/share/67cafbe1-6428-8010-8733-129475eb5ce8).

We used Python scripts `comments-filter-fields-chunk.py` and `submission-filter-chunk.py` to select the necessary fields from the raw data, and exported the results file to `<theme>/<theme>_comments.jsonl` and `<theme>/<theme>_submissions.jsonl`.

After the processing, the size of `unpopularopinion_comments.jsonl` and `unpopularopinion_submissions.jsonl` shrinked to 14G and 1.9G, respectively. These files are much smaller in size, much more concise and well-formatted in keys.

#### Count user comments and posts count
From now on, we work on `<theme>/<theme>_comments.jsonl` and `<theme>/<theme>_submissions.jsonl`, rather than the raw data.

We can count #post of each user from `<theme>/<theme>_submissions.jsonl` and calculate #comments, #comments_on_unique_posts from `<theme>/<theme>_comments.jsonl`, using `count-submission.py` and `count-comment.py`, respectively. After this, we got `<theme>/<theme>_user_comment_count.tsv` and `<theme>/<theme>_user_post_count.tsv`

Finally, we run `count-summary.py`, using the two files generated just now, to get the `<theme>/<theme>_user_summary.tsv`


#### Into database

As the size of `unpopularopinion_comments.jsonl` (14G) and `unpopularopinion_submissions.jsonl` (1.9G) is still quite big to operate directly in memory, it's a good idea to put them into database for quick retrieval. Running `comments-db.py` and `submissions-db.py` and we can get the database version of the comments and submissions (`unpopularopinion_comments.db` and `unpopularopinion_submissions.db`), which offers a much more feasible solution for situations where comments and submissions files are too big.

