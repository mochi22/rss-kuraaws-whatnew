import unittest
from unittest.mock import MagicMock, patch
import boto3
from moto import mock_dynamodb
from datetime import datetime, timedelta
import time
from rss_aws_whatsnew.src.dynamodb import FeedEntryDB


def wait_for_table(table):
    while True:
        try:
            table.describe_table()
            print("Table exists and is ready")
            break
        except table.meta.client.exceptions.ResourceNotFoundException:
            print("Table is not ready yet, waiting...")
            time.sleep(5)


# テストケースの定義
class TestFeedEntryDB(unittest.TestCase):

    def setUp(self):
        # DynamoDBのモック
        self.mock_dynamodb = mock_dynamodb()
        self.mock_dynamodb.start()
        self.dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        self.old_day = 7
        # get now
        self.now = datetime.now()
        self.struct_now = time.localtime(time.mktime(self.now.timetuple()))

        # get self.old_day+1 day
        self.n_days_ago = self.now - timedelta(days=self.old_day + 1)
        self.struct_n_days_ago = time.localtime(
            time.mktime(self.n_days_ago.timetuple())
        )

        # fmt: off
        # テスト用のテーブル作成
        self.table_name = "test-table"
        self.table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {
                    "AttributeName": "id",
                    "KeyType": "HASH"
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "id",
                    "AttributeType": "S"
                }
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5
            },
        )
        # fmt: on
        wait_for_table(self.table)  # テーブルが利用可能になるまで待機

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
        print("start test test_save_entries")
        # モックのRSSフィードエントリ
        mock_feed = MagicMock()
        # fmt: off
        mock_feed.entries = [
            {
                "id": "1",
                "title": "Test Entry 1",
                "published": 'Mon, 01 Jan 2023 00:00:00 +0000',
                'published_parsed': self.struct_now
            },
            {
                "id": "2",
                "title": "Test Entry 2",
                "published": 'Jun, 01 Jan 2024 00:00:00 +0000',
                'published_parsed': self.struct_n_days_ago
            },
        ]
        # fmt: on

        # エントリの保存
        self.feed_entry_db.save_entries(mock_feed)

        # テーブルからデータを取得
        response = self.table.scan()
        print("res", response)
        items = response["Items"]

        # エントリが正しく保存されていることを確認
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["id"], "1")
        self.assertEqual(items[0]["title"], "Test Entry 1")
        self.assertEqual(
            items[0]["published_parsed"].strftime("%Y-%m-%d %H:%M:%S"),
            "2023-01-01 00:00:00",
        )
        self.assertEqual(items[1]["id"], "2")
        self.assertEqual(items[1]["title"], "Test Entry 2")
        self.assertEqual(
            items[1]["published_parsed"].strftime("%Y-%m-%d %H:%M:%S"),
            "2024-01-01 00:00:00",
        )

    def test_get_recent_entries(self):
        print("start test test_get_recent_entries")
        # テストデータの準備
        self.table.put_item(
            Item={
                "id": "1",
                "title": "Recent Entry",
                "published_parsed": self.struct_now,
            }
        )
        self.table.put_item(
            Item={
                "id": "2",
                "title": "Old Entry",
                "published_parsed": self.struct_n_days_ago,
            }
        )

        # 最近7日間のエントリを取得
        recent_entries = self.feed_entry_db.get_recent_entries(days=7)

        # 結果の検証
        self.assertEqual(len(recent_entries), 1)
        self.assertEqual(recent_entries[0]["id"], "1")
        self.assertEqual(recent_entries[0]["title"], "Recent Entry")

    def test_delete_old_entries(self):
        self.table.put_item(
            Item={
                "id": "1",
                "title": "Recent Entry",
                "summary": "Recent Summary",
                "published": "Mon, 01 Jan 2023 00:00:00 +0000",
                "published_parsed": self.struct_now,
            },
        )
        self.table.put_item(
            Item={
                "id": "2",
                "title": "Old Entry",
                "summary": "Old Summary",
                "published": "Mon, 01 Jan 2022 00:00:00 +0000",
                "published_parsed": self.struct_n_days_ago,
            }
        )

        # 7日以上前のエントリを削除
        self.feed_entry_db.delete_old_entries(days=7)

        # 結果の検証
        response = self.table.scan()
        items = response["Items"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], "1")
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
