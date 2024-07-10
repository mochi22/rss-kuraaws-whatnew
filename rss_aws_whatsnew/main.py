import sqlite3
import feedparser

from db import FeedEntryDB



DEBUG = True

# RSSフィードを取得し、データベースに保存
def main():
    feed_url = "https://aws.amazon.com/new/feed" #
    feed_url2 = "https://aws.amazon.com/jp/about-aws/whats-new/recent/feed/" #links: 「https://aws.amazon.com/jp/about-aws/whats-new/recent/」みたいな感じでちょっと違う
    feed_url3 = "https://aws.amazon.com/blogs/aws/feed/" # これは上記二つと違う
    
    path = "./"
    db_name = path + 'aws_feed.db'
    if DEBUG:
        print("db path:", db_name)
    
    # create db object and init db
    db = FeedEntryDB(db_name)
    db.init_db()

    # get rss feed
    feed = feedparser.parse(feed_url)


    if DEBUG:
        print("getting feed size:", len(feed))

    # insert articols
    db.save_entries(feed)

    new_entries = db.get_recent_entries(8)

    for entry in new_entries:
        if db.contains_service_word(entry[2]):
            print("Title:", entry[2])
            #print("Summary:", entry[4])
            #print("Published:", entry[7])
            #print("Link:", entry[13])
            #print("tag", entry[8])
            print("-" * 30)

    # delete old articols
    # db.delete_old_entries(days=10)

if __name__ == "__main__":
    main()



