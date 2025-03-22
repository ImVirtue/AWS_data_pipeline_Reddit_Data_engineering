from utils.constants import CLIENT_ID, SECRET
from etls.reddit_etl import connect_reddit, extract_posts, transform_data
from etls.interact_with_db import insert_reddit_post, create_rds_connection, create_table


def reddit_pipeline(limit=1000):
    #connection to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'Windows:RedditScraper:1.0 (by /u/Electrical-Cap5118)')

    #extraction
    posts = extract_posts(instance, limit)

    #transform
    posts = transform_data(posts)

    #load into db
    conn = create_rds_connection()

    create_table(conn)

    insert_reddit_post(posts, conn)

