import sys
import numpy as np
import pandas as pd
import praw
from praw import Reddit
from datetime import datetime, timedelta

from utils.constants import POST_FIELDS


def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = praw.Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent)
        print("connected to reddit!")
        return reddit
    except Exception as e:
        print(e)
        sys.exit(1)


def extract_posts(reddit_instance: Reddit, limit=1000):
    subreddits = ['dataengineering', 'leagueoflegends', 'databricks']
    now = datetime.utcnow()
    yesterday_start = datetime(now.year, now.month, now.day) - timedelta(days=1)
    yesterday_end = yesterday_start + timedelta(days=1)

    start_timestamp = int(yesterday_start.timestamp())
    end_timestamp = int(yesterday_end.timestamp())

    posts_list = []
    for subreddit in subreddits:
        subreddit = reddit_instance.subreddit(subreddit)
        posts = subreddit.new(limit=limit)
        for post in posts:
            dt = datetime.fromtimestamp(post.created_utc)  # UTC time
            created_time = dt.strftime('%Y-%m-%d %H:%M:%S')
            if start_timestamp <= int(post.created_utc) < end_timestamp:
                post_dict = vars(post)
                post = {key: post_dict[key] for key in POST_FIELDS}
                post['topic'] = subreddit
                post['created_time'] = created_time
                posts_list.append(post)

    return posts_list


def transform_data(posts_list):
    post_df = pd.DataFrame(posts_list)
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['topic'] = post_df['topic'].astype(str)
    post_df['title'] = post_df['title'].astype(str)

    posts_list = [tuple(row) for row in post_df.values.tolist()]

    return posts_list


