import sqlite3
from datetime import datetime, timedelta
import time
import re


class FeedEntryDB:
    """
    define DB objects
    """

    def __init__(self, db_name):
        self.db_name = db_name
        # self.init_db()
        # "OpenSearch Service", "OpenSearch Service - Ingestion", "OpenSearch Service - Serverless", "OpenSearch Service - Managed Cluster" -> "OpenSearch"  # noqa: E501
        # "Managed Service for Prometheus", Grafana, Kafka -> Prometheus, Grafana, Kafka  # noqa: E501
        # Kinesis Analytics", "Kinesis Firehose", "Kinesis Streams" -> "Kinesis" # noqa: E501
        self.target_services = [
            "Kinesis", "OpenSearch", "QuickSight", "Redshift", "Kendra",
            "AppFlow", "DataZone", "Grafana", "Prometheus", "Kafka", "Neptune",
            "CloudSearch", "Q Business",
            "Supply Chain", "Honeycode", "Lookout for Metrics"
        ]

    def init_db(self):
        """
        init sqlite db and create table
        """

        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        c.execute(
            '''
                CREATE TABLE IF NOT EXISTS feed_entries
                    (
                        id TEXT PRIMARY KEY,
                        guidislink TEXT,
                        title TEXT, title_detail TEXT,
                        summary TEXT,
                        summary_detail TEXT,
                        published TEXT,
                        published_parsed TEXT,
                        tags TEXT,
                        authors TEXT,
                        author TEXT,
                        author_detail TEXT,
                        links TEXT,
                        link TEXT
                    )
            '''
        )
        conn.commit()
        conn.close()

    def save_entries(self, feed):
        """
        loading rss feed data and insert to sqlite table.
        """

        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        for entry in feed.entries:
            published_parsed = entry.get('published_parsed', None)
            if published_parsed:
                published_parsed = datetime.fromtimestamp(
                                    time.mktime(published_parsed)
                                    ).strftime('%Y-%m-%d %H:%M:%S')
            else:
                published_parsed = ''
            values = (
                entry.get('id', ''),
                entry.get('guidislink', ''),
                entry.get('title', ''),
                str(entry.get('title_detail', {})),
                entry.get('summary', ''),
                str(entry.get('summary_detail', {})),
                entry.get('published', ''),
                published_parsed,
                str(entry.get('tags', [])),
                str(entry.get('authors', [])),
                entry.get('author', ''),
                str(entry.get('author_detail', {})),
                str(entry.get('links', [])),
                entry.get('link', '')
            )
            try:
                c.execute(
                        "INSERT OR REPLACE INTO feed_entries VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values
                )
            except sqlite3.ProgrammingError as e:
                print(f"データ形式が変更された可能性があります: {e}")
        conn.commit()
        conn.close()

    def get_recent_entries(self, days=7):
        """
        get articles last n days
        """

        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        recent_date = datetime.now() - timedelta(days=days)
        print("recent_date:", recent_date)
        c.execute(
                "SELECT * FROM feed_entries WHERE published_parsed >= ?",
                (recent_date.strftime('%Y-%m-%d %H:%M:%S'),)
        )
        rows = c.fetchall()
        print("rows", len(rows))
        conn.close()
        return rows

    def delete_old_entries(self, days=30):
        """
        delete articles before n
        """

        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        #  get table length before deleting
        c.execute(
                "SELECT COUNT(*) FROM feed_entries"
        )
        table_size_before_delete = c.fetchone()[0]
        recent_date = datetime.now() - timedelta(days=days)
        print("recent_date:", recent_date)
        c.execute(
                "DELETE FROM feed_entries WHERE published_parsed < ?",
                (recent_date.strftime('%Y-%m-%d %H:%M:%S'),)
        )
        conn.commit()
        #  get table length after deleting
        c.execute("SELECT COUNT(*) FROM feed_entries")
        table_size_after_delete = c.fetchone()[0]
        print("deleting:", table_size_before_delete - table_size_after_delete)
        conn.close()

    def contains_service_word(self, text):
        for service_words in self.target_services:
            pattern = r'\b' + re.escape(service_words) + r'\b'
            if re.search(pattern, text, re.IGNORECASE):
                #  re.escapeを使って正規表現のメタ文字をエスケープしています。
                #  \bで単語の境界を指定して、サービス名の単語が含まれる場合のみマッチ
                #  re.IGNORECASEを指定することで、大文字小文字を区別せずに検索
                print("service true:", service_words)
                return True
        return False
