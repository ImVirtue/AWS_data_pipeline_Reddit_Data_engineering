import psycopg2
from utils.constants import DATABASE_NAME, DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASSWORD

def create_rds_connection():
    conn = psycopg2.connect(
        dbname = DATABASE_NAME,
        user = DATABASE_USER,
        password = DATABASE_PASSWORD,
        host = DATABASE_HOST,
        # host = 'localhost',
        port = DATABASE_PORT
    )

    return conn

def create_table(conn):
    cur = conn.cursor()

    create_table_query = """
        CREATE TABLE IF NOT EXISTS reddit_posts (
            id TEXT PRIMARY KEY,
            title TEXT,
            score INT,
            num_comments INT,
            author varchar(255),
            created_utc TIMESTAMP,
            url TEXT,
            over_18 BOOLEAN,
            edited BOOLEAN,
            spoiler BOOLEAN,
            stickied BOOLEAN,
            topic TEXT,
            post_created_time TIMESTAMP,
            created_time TIMESTAMP
        );
    """
    try:
        cur.execute(create_table_query)
        conn.commit()
    except Exception as e:
        print(e)


def insert_reddit_post(data, conn):
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO reddit_posts (
        id, title, score, num_comments, author, created_utc, url, over_18, edited, spoiler, stickied, topic, post_created_time, created_time
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
    );
    """
    try:
        cursor.executemany(insert_query, data)
        conn.commit()

    except Exception as e:
        print("Lỗi khi chèn dữ liệu:", e)
        conn.rollback()

    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    conn = create_rds_connection()
    print(conn)
