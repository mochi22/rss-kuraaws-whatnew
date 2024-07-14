import boto3
from typing import Optional
import feedparser
import os
import base64
import requests
from dynamodb import FeedEntryDB


#  RSSフィードを取得し、データベースに保存
def lambda_handler(event, context):
    feed_url = "https://aws.amazon.com/new/feed"
    #  "https://aws.amazon.com/jp/about-aws/whats-new/recent/feed/"
    #  "https://aws.amazon.com/blogs/aws/feed/"
    # line_notify_token = os.environ.get('LINE_NOTIFY_TOKEN')
    line_notify_token = get_enctypted_env_variables("LINE_NOTIFY_TOKEN")
    if line_notify_token:
        print(f"Decrypted API token: {line_notify_token}")
    else:
        print("Failed to decrypt API token.", line_notify_token)
    db_name = "aws_whatsnew_feed"

    #  create db object and init db
    db = FeedEntryDB(db_name)

    #  get rss feed
    feed = feedparser.parse(feed_url)

    #  insert articols
    db.save_entries(feed)

    #  get newest articles
    new_entries = db.get_recent_entries(8)

    for entry in new_entries:
        if db.contains_service_word(entry["title"]):
            print("Title:", entry["title"])
            #  print("Summary:", entry[4])
            #  print("Published:", entry[7])
            #  print("Link:", entry[13])
            #  print("tag", entry[8])
            print("-" * 30)
            # Line Notifyで通知
            """
            # message = f"
                        Title: {entry['title']}\n
                        Summary: {entry['summary']}\n
                        Published: {entry['published']}\n
                        Link: {entry['link']}\n
                        "
            """
            # print("message:", message)
            message2 = f"Title: {entry['title']}\nLink: {entry['link']}\n"
            print("message2:", message2)
            send_line_notify(line_notify_token, message2)
    #  delete old articols
    #  db.delete_old_entries(days=10)
    return {"statusCode": 200, "body": "Successfully processed feed entries."}


def get_enctypted_env_variables(encrypted_token_name: str) -> Optional[str]:
    """
    Decrypts the encrypted value of an environment variable.

    Args:
        encrypted_token_name (str): The name of the environment variable
                                    containing the encrypted value.

    Returns:
        Optional[str]: The decrypted value. Returns None if decryption fails.
    """
    encrypted_token = os.environ.get(encrypted_token_name)
    if not encrypted_token:
        raise ValueError(
                f"{encrypted_token_name} environment variable is required."
        )

    kms_client = boto3.client("kms")
    try:
        decoded_token = base64.b64decode(encrypted_token)
        decrypted_token = kms_client.decrypt(
            CiphertextBlob=decoded_token,
            EncryptionContext={
                "LambdaFunctionName": os.environ["AWS_LAMBDA_FUNCTION_NAME"]
            },
        )["Plaintext"].decode("utf-8")
        return decrypted_token
    except Exception as e:
        print(f"Error decrypting token: {e}")
        return None


def send_line_notify(token: str, message: str) -> Optional[requests.Response]:
    """
    Send a message to LINE Notify.

    Args:
        token (str): LINE Notify API token
        message (str): Message to be sent

    Returns:
        Optional[requests.Response]: Response object
                                    if the request is success, else None
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    params = {"message": message}
    url = "https://notify-api.line.me/api/notify"

    try:
        response = requests.post(url, headers=headers, params=params)
        response.raise_for_status()
        print("LINE Notify message sent successfully.")
        return response
    except requests.exceptions.RequestException as e:
        print(f"Failed to send LINE Notify message: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return None
