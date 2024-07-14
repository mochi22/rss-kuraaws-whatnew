import boto3
from datetime import datetime, timedelta
import re
from typing import List


class FeedEntryDB:
    """
    Define DynamoDB operations for feed entries.
    """

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.dynamodb = boto3.resource("dynamodb")
        self.table = self.get_or_create_table(table_name)
        self.target_services = [
            "Kinesis",
            "OpenSearch",
            "QuickSight",
            "Redshift",
            "Kendra",
            "AppFlow",
            "DataZone",
            "Grafana",
            "Prometheus",
            "Kafka",
            "Neptune",
            "CloudSearch",
            "Q Business",
            "Supply Chain",
            "Honeycode",
            "Lookout for Metrics",
        ]

    def get_or_create_table(self, table_name: str):
        """
        Get an existing DynamoDB table or create a new one.

        Args:
            table_name (str): Name of the DynamoDB table.

        Returns:
            dynamodb.Table: DynamoDB table instance.
        """
        existing_tables = [table.name for table in self.dynamodb.tables.all()]
        if table_name in existing_tables:
            table = self.dynamodb.Table(table_name)
            print(f"Table '{table_name}' already exists.")
        else:
            table = self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            )
            table.wait_until_exists()
            print(f"Table '{table_name}' created successfully.")
        return table

    def save_entries(self, feed) -> None:
        """
        Load RSS feed data and insert into the DynamoDB table.

        Args:
            feed: RSS feed object.
        """
        for entry in feed.entries:
            published_parsed = self.parse_published_date(entry.get("published_parsed"))
            item = {
                "id": entry.get("id", ""),
                "guidislink": entry.get("guidislink", ""),
                "title": entry.get("title", ""),
                "title_detail": str(entry.get("title_detail", {})),
                "summary": entry.get("summary", ""),
                "summary_detail": str(entry.get("summary_detail", {})),
                "published": entry.get("published", ""),
                "published_parsed": published_parsed,
                "tags": str(entry.get("tags", [])),
                "authors": str(entry.get("authors", [])),
                "author": entry.get("author", ""),
                "author_detail": str(entry.get("author_detail", {})),
                "links": str(entry.get("links", [])),
                "link": entry.get("link", ""),
            }
            try:
                self.table.put_item(Item=item)
            except Exception as e:
                print(f"データ形式が変更された可能性があります: {e}")

    @staticmethod
    def parse_published_date(published_parsed) -> str:
        """
        Parse the published date from the feed entry.

        Args:
            published_parsed: Published date object from the feed entry.

        Returns:
            str: Formatted published date string.
        """
        if published_parsed:
            return datetime.fromtimestamp(time.mktime(published_parsed)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        return ""

    def get_recent_entries(self, days: int = 7) -> List:
        """
        Get articles published in the last n days.

        Args:
            days (int): Number of days to look back.

        Returns:
            List: List of feed entry items.
        """
        recent_date = datetime.now() - timedelta(days=days)
        print("recent_date:", recent_date)
        response = self.table.scan(
            FilterExpression="published_parsed >= :recent_date",
            ExpressionAttributeValues={
                ":recent_date": recent_date.strftime("%Y-%m-%d %H:%M:%S")
            },
        )
        items = response["Items"]
        print("items", len(items))
        return items

    def delete_old_entries(self, days: int = 30) -> None:
        """
        Delete articles older than n days.

        Args:
            days (int): Number of days to keep entries.
        """
        recent_date = datetime.now() - timedelta(days=days)
        print("recent_date:", recent_date)
        table_size_before_delete = self.table.item_count
        response = self.table.scan(
            FilterExpression="published_parsed < :recent_date",
            ExpressionAttributeValues={
                ":recent_date": recent_date.strftime("%Y-%m-%d %H:%M:%S")
            },
            ProjectionExpression="id",
        )
        with self.table.batch_writer() as batch:
            for item in response["Items"]:
                batch.delete_item(Key={"id": item["id"]})
        table_size_after_delete = self.table.item_count
        print("deleting:", table_size_before_delete - table_size_after_delete)

    def contains_service_word(self, text: str) -> bool:
        """
        Check if the given text contains any of the target service words.

        Args:
            text (str): Text to search for service words.

        Returns:
            bool: True if any service word is found, False otherwise.
        """
        for service_word in self.target_services:
            pattern = r"\b" + re.escape(service_word) + r"\b"
            if re.search(pattern, text, re.IGNORECASE):
                print("service true:", service_word)
                return True
        return False
