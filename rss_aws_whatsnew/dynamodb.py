import boto3
import time
from datetime import datetime, timedelta
import re

class FeedEntryDB:
    """
    define DB objects
    """

    def __init__(self, table_name):
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb')
        # テーブル名のリストを取得
        table_names = [table.name for table in self.dynamodb.tables.all()]
        if table_name in table_names:
            self.table = self.dynamodb.Table(self.table_name)
        else:
            self.table = self.get_or_create_table(self.table_name)
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
    def get_or_create_table(self, table_name):
        try:
            table = self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'id',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'id',
                        'AttributeType': 'S'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            table.wait_until_exists()
            print(f"Table '{table_name}' created successfully.")
        except self.dynamodb.meta.client.exceptions.ResourceInUseException:
            table = self.dynamodb.Table(table_name)
            print(f"Table '{table_name}' already exists.")
        return table

    def save_entries(self, feed):
        """
        loading rss feed data and insert to DynamoDB table.
        """

        for entry in feed.entries:
            published_parsed = entry.get("published_parsed", None)
            if published_parsed:
                published_parsed = datetime.fromtimestamp(
                    time.mktime(published_parsed)
                ).strftime("%Y-%m-%d %H:%M:%S")
            else:
                published_parsed = ""
            item = {
                'id': entry.get("id", ""),
                'guidislink': entry.get("guidislink", ""),
                'title': entry.get("title", ""),
                'title_detail': str(entry.get("title_detail", {})),
                'summary': entry.get("summary", ""),
                'summary_detail': str(entry.get("summary_detail", {})),
                'published': entry.get("published", ""),
                'published_parsed': published_parsed,
                'tags': str(entry.get("tags", [])),
                'authors': str(entry.get("authors", [])),
                'author': entry.get("author", ""),
                'author_detail': str(entry.get("author_detail", {})),
                'links': str(entry.get("links", [])),
                'link': entry.get("link", ""),
            }
            try:
                self.table.put_item(Item=item)
            except Exception as e:
                print(f"データ形式が変更された可能性があります: {e}")

    def get_recent_entries(self, days=7):
        """
        get articles last n days
        """

        recent_date = datetime.now() - timedelta(days=days)
        print("recent_date:", recent_date)
        response = self.table.scan(
            FilterExpression=f"published_parsed >= :recent_date",
            ExpressionAttributeValues={":recent_date": recent_date.strftime("%Y-%m-%d %H:%M:%S")},
        )
        items = response["Items"]
        print("items", len(items))
        return items

    def delete_old_entries(self, days=30):
        """
        delete articles before n
        """

        recent_date = datetime.now() - timedelta(days=days)
        print("recent_date:", recent_date)
        table_size_before_delete = self.table.item_count
        response = self.table.scan(
            FilterExpression=f"published_parsed < :recent_date",
            ExpressionAttributeValues={":recent_date": recent_date.strftime("%Y-%m-%d %H:%M:%S")},
            ProjectionExpression="id",  # 削除対象のIDのみを取得するように指定
        )
        with self.table.batch_writer() as batch:
            for item in response["Items"]:
                batch.delete_item(Key={"id": item["id"]})
        table_size_after_delete = self.table.item_count
        print("deleting:", table_size_before_delete - table_size_after_delete)

    def contains_service_word(self, text):
        for service_words in self.target_services:
            pattern = r"\b" + re.escape(service_words) + r"\b"
            if re.search(pattern, text, re.IGNORECASE):
                #  re.escapeを使って正規表現のメタ文字をエスケープしています。
                #  \bで単語の境界を指定して、サービス名の単語が含まれる場合のみマッチ
                #  re.IGNORECASEを指定することで、大文字小文字を区別せずに検索
                print("service true:", service_words)
                return True
        return False
