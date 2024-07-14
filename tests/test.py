import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from moto import mock_aws as mock_dynamodb
import boto3
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# FeedEntryDBクラスをインポート
from rss_aws_whatsnew.src.dynamodb import FeedEntryDB


# テストケースの定義
class TestFeedEntryDB(unittest.TestCase):

    def setUp(self):
        # DynamoDBのモック
        self.mock_dynamodb = mock_dynamodb()
        self.mock_dynamodb.start()
        self.dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

        # テスト用のテーブル作成
        self.table_name = "test-table"
        self.table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        self.table.meta.client.get_waiter("table_exists").wait(
            TableName=self.table_name
        )

        # FeedEntryDBクラスのインスタンス作成
        self.feed_entry_db = FeedEntryDB(self.table_name)

    def tearDown(self):
        # モックの終了
        self.mock_dynamodb.stop()

    def test_get_or_create_table(self):
        # テーブルが存在する場合
        table = self.feed_entry_db.get_or_create_table(self.table_name)
        self.assertEqual(table.name, self.table_name)

        # テーブルが存在しない場合
        new_table_name = "new-test-table"
        with patch("builtins.print") as mock_print:
            table = self.feed_entry_db.get_or_create_table(new_table_name)
            mock_print.assert_called_with(
                f"Table '{new_table_name}' created successfully."
            )
        self.assertEqual(table.name, new_table_name)

    def test_save_entries(self):
        # モックのRSSフィードエントリ
        mock_feed = MagicMock()
        mock_feed.entries = [
            {
                "id": "1",
                "title": "Test Entry 1",
                "published_parsed": (2023, 5, 1, 10, 0, 0, 0, 0, 0),
            },
            {
                "id": "2",
                "title": "Test Entry 2",
                "published_parsed": (2023, 5, 2, 12, 0, 0, 0, 0, 0),
            },
        ]

        # エントリの保存
        self.feed_entry_db.save_entries(mock_feed)

        # テーブルからデータを取得
        response = self.table.scan()
        items = response["Items"]

        # エントリが正しく保存されていることを確認
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["id"], "1")
        self.assertEqual(items[0]["title"], "Test Entry 1")
        self.assertEqual(items[0]["published_parsed"], "2023-05-01 10:00:00")
        self.assertEqual(items[1]["id"], "2")
        self.assertEqual(items[1]["title"], "Test Entry 2")
        self.assertEqual(items[1]["published_parsed"], "2023-05-02 12:00:00")

    def test_get_recent_entries(self):
        # テストデータの準備
        now = datetime.now()
        self.table.put_item(
            Item={
                "id": "1",
                "title": "Old Entry",
                "published_parsed": (now - timedelta(days=8)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            }
        )
        self.table.put_item(
            Item={
                "id": "2",
                "title": "Recent Entry",
                "published_parsed": (now - timedelta(days=3)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            }
        )

        # 最近7日間のエントリを取得
        recent_entries = self.feed_entry_db.get_recent_entries(days=7)

        # 結果の検証
        self.assertEqual(len(recent_entries), 1)
        self.assertEqual(recent_entries[0]["id"], "2")
        self.assertEqual(recent_entries[0]["title"], "Recent Entry")

    def test_delete_old_entries(self):
        # テストデータの準備
        now = datetime.now()
        self.table.put_item(
            Item={
                "id": "1",
                "title": "Old Entry",
                "published_parsed": (now - timedelta(days=40)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            }
        )
        self.table.put_item(
            Item={
                "id": "2",
                "title": "Recent Entry",
                "published_parsed": (now - timedelta(days=10)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            }
        )

        # 30日以上前のエントリを削除
        self.feed_entry_db.delete_old_entries(days=30)

        # 結果の検証
        response = self.table.scan()
        items = response["Items"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], "2")
        self.assertEqual(items[0]["title"], "Recent Entry")

    def test_contains_service_word(self):
        # サービス名が含まれる場合
        text = "This is a test for OpenSearch service."
        self.assertTrue(self.feed_entry_db.contains_service_word(text))

        # サービス名が含まれない場合
        text = "This is a test without any service name."
        self.assertFalse(self.feed_entry_db.contains_service_word(text))


if __name__ == "__main__":
    unittest.main()
