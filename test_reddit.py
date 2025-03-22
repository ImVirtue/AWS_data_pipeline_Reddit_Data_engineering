import praw
from datetime import datetime, timedelta
import pandas as pd

user_agent = "Windows:RedditScraper:1.0 (by /u/Electrical-Cap5118)"
# reddit = praw.Reddit(
#     client_id="npuTuIap12Sv4iOOWrtG2A",
#     client_secret="pmqPCcFDmfwpjPcilUt2T3ov4Ouf6w",
#     user_agent=user_agent
# )

# print(type(reddit))

headlines = set()
cnt = 0

def fetch_reddit_yesterday(subreddit_name):
    reddit = praw.Reddit(
        client_id="npuTuIap12Sv4iOOWrtG2A",
        client_secret="pmqPCcFDmfwpjPcilUt2T3ov4Ouf6w",
        user_agent=user_agent
    )

    # Xác định thời gian của ngày hôm qua
    now = datetime.utcnow()
    yesterday_start = datetime(now.year, now.month, now.day) - timedelta(days=1)
    yesterday_end = yesterday_start + timedelta(days=1)

    start_timestamp = int(yesterday_start.timestamp())
    end_timestamp = int(yesterday_end.timestamp())
    print(start_timestamp, end_timestamp)

    print(f"🔍 Đang lấy bài viết từ {yesterday_start} đến {yesterday_end}")

    # subreddit_name = "leagueoflegend"  # Đổi theo subreddit bạn muốn
    subreddit = reddit.subreddit(subreddit_name)

    posts = []
    for post in subreddit.new(limit=1000):  # Lấy nhiều bài viết để lọc
        dt = datetime.fromtimestamp(post.created_utc)  # UTC time
        created_time = dt.strftime('%Y-%m-%d %H:%M:%S')
        if start_timestamp <= int(post.created_utc) < end_timestamp:
            posts.append({
                "title": post.title,
                "url": post.url,
                "score": post.score,
                "topic": subreddit_name,
                "created_time": created_time
            })
        posts_df = pd.DataFrame(posts)

    return posts_df

if __name__ == '__main__':
    posts_df = fetch_reddit_yesterday('dataengineering')
    print(posts_df)



# for submission in reddit.subreddit('dataengineering').hot(limit=None):
#     print(submission)
#     print(type(submission))
#     print(submission.title)
#     print(submission.id)
#     print(submission.author)
#     print(submission.created_utc)
#     print(submission.score)
#     print(submission.upvote_ratio)
#     print(submission.url)
#     print(submission.is_video)
#     # cnt += 1
#     # if cnt == 10:
#     break
    # headlines.add(submission.title)

# print(len(headlines))