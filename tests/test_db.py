import unittest
from unittest.mock import Mock
import sqlite3
import datetime
import time
import sys
from rss_aws_whatsnew import db


sys.path.append("/home/ec2-user/rss-aws-whatsnew/")


class TestFeedEntryDB(unittest.TestCase):

    def setUp(self):
        self.db_name = "test.db"
        self.db = db.FeedEntryDB(self.db_name)
        self.db.init_db()
        self.old_day = 7
        # print(f"Start test: {self._testMethodName}")  # テスト開始時の出力

    def tearDown(self):
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS feed_entries")
        conn.commit()
        conn.close()
        # print(f"End test: {self._testMethodName}")  # テスト終了時の出力

    def test_init_db(self):
        """
        test feed_entries tables
        """
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        c.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='feed_entries'"
        )
        table_exists = c.fetchone() is not None
        conn.close()
        self.assertTrue(table_exists)

    def test_save_entries(self):
        mock_feed = Mock()

        # get now
        now = datetime.datetime.now()
        struct_now = time.localtime(time.mktime(now.timetuple()))

        mock_feed.entries = [
            {
                "id": "1",
                "title": "Test Entry",
                "summary": "Test Summary",
                "published": "Mon, 01 Jan 2023 00:00:00 +0000",
                "published_parsed": struct_now,
            }
        ]

        self.db.save_entries(mock_feed)

        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM feed_entries")
        entry_count = c.fetchone()[0]
        conn.close()

        self.assertEqual(entry_count, 1)

    def test_get_recent_entries(self):
        mock_feed = Mock()

        # get now
        now = datetime.datetime.now()
        struct_now = time.localtime(time.mktime(now.timetuple()))

        # get old_day+1 day
        n_days_ago = now - datetime.timedelta(days=self.old_day + 1)
        struct_n_days_ago = time.localtime(time.mktime(n_days_ago.timetuple()))

        mock_feed.entries = [
            {
                "id": "1",
                "title": "Recent Entry",
                "summary": "Recent Summary",
                "published": "Mon, 01 Jan 2023 00:00:00 +0000",
                "published_parsed": struct_now,
            },
            {
                "id": "2",
                "title": "Old Entry",
                "summary": "Old Summary",
                "published": "Mon, 01 Jan 2022 00:00:00 +0000",
                "published_parsed": struct_n_days_ago,
            },
        ]

        self.db.save_entries(mock_feed)

        recent_entries = self.db.get_recent_entries(days=7)
        self.assertEqual(len(recent_entries), 1)
        self.assertEqual(recent_entries[0][2], "Recent Entry")

    def test_delete_old_entries(self):
        mock_feed = Mock()

        # get now
        now = datetime.datetime.now()
        struct_now = time.localtime(time.mktime(now.timetuple()))

        # get self.old_day+1 day
        n_days_ago = now - datetime.timedelta(days=self.old_day + 1)
        struct_n_days_ago = time.localtime(time.mktime(n_days_ago.timetuple()))

        mock_feed.entries = [
            {
                "id": "1",
                "title": "Recent Entry",
                "summary": "Recent Summary",
                "published": "Mon, 01 Jan 2023 00:00:00 +0000",
                "published_parsed": struct_now,
            },
            {
                "id": "2",
                "title": "Old Entry",
                "summary": "Old Summary",
                "published": "Mon, 01 Jan 2022 00:00:00 +0000",
                "published_parsed": struct_n_days_ago,
            },
        ]

        self.db.save_entries(mock_feed)

        self.db.delete_old_entries(days=7)

        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM feed_entries")
        entry_count = c.fetchone()[0]
        conn.close()

        self.assertEqual(entry_count, 1)

    def test_contains_service_word(self):
        self.assertTrue(self.db.contains_service_word("Kinesis test"))
        self.assertTrue(self.db.contains_service_word("OpenSearch Service"))
        self.assertFalse(self.db.contains_service_word("No service name"))


if __name__ == "__main__":
    unittest.main()
