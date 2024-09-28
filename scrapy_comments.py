import datetime
import os

import dotenv
import pandas as pd
from googleapiclient.discovery import build
from tqdm import tqdm

dotenv.load_dotenv()

API_KEY = os.getenv("API_KEY")
VIDEO_ID = str(input("Please write URL and push Enter: ").split('=')[1])

comments_list = []


class ScrapyComments:
    """
    Scrapes youtube comments and checks whether a user  commented on the
    given videos.
    """
    def __init__(self, api_key, video_id):
        self.api_key = api_key
        self.video_id = video_id
        self.connection = build('youtube', 'v3', developerKey=self.api_key)

    def collect_comments(self, resp):
        """
        Reade comment and answer from response.
        :param resp:
        :return:
        """
        for item in tqdm(resp['items']):
            name = item["snippet"]['topLevelComment']["snippet"][
                "authorDisplayName"]
            comment = item["snippet"]['topLevelComment']["snippet"][
                "textDisplay"]
            published_at = item["snippet"]['topLevelComment']["snippet"][
                'publishedAt']
            likes = item["snippet"]['topLevelComment']["snippet"]['likeCount']
            replies = item["snippet"]['totalReplyCount']
            if [name, comment, published_at, likes,
                    replies] not in comments_list:
                comments_list.append(
                    [name, comment, published_at, likes, replies])

            totalReplyCount = item["snippet"]['totalReplyCount']
            if totalReplyCount > 0:
                parent = item["snippet"]['topLevelComment']["id"]
                response_2 = self.connection.comments().list(
                    part='snippet',
                    maxResults='100',
                    parentId=parent,
                    textFormat="plainText").execute()

                for i in response_2["items"]:
                    name = i["snippet"]["authorDisplayName"]
                    comment = i["snippet"]["textDisplay"]
                    published_at = i["snippet"]['publishedAt']
                    likes = i["snippet"]['likeCount']
                    replies = ""
                    if [name, comment, published_at, likes,
                            replies] not in comments_list:
                        comments_list.append(
                            [name, comment, published_at, likes, replies])

    def save_to_csv(self):
        """
        Save comments to CSV-file.
        :return:
        """
        df = pd.DataFrame({'Name': [i[0] for i in comments_list],
                           'Comment': [i[1] for i in comments_list],
                           'Date': [i[2] for i in comments_list],
                           'Likes': [i[3] for i in comments_list],
                           'Reply Count': [i[4] for i in comments_list]})
        date = datetime.date.today().__str__().replace("-", "_")
        df.to_csv(
            f"{VIDEO_ID}_{date}.csv",
            mode="a", sep=";",
            header=True,
            index=False,
            encoding="utf-16"
        )
        return "Successful! Check the CSV file  created."

    def get_comments(self):
        """
        Collect comments from Yatube.
        :return:
        """
        response = self.connection.commentThreads().list(
            part='snippet, replies', videoId=self.video_id).execute()
        self.collect_comments(response)
        while 'nextPageToken' in response:
            response = self.connection.commentThreads().list(
                part='snippet,replies', videoId=self.video_id,
                pageToken=response["nextPageToken"]).execute()
            self.collect_comments(response)

        print('Count  comments: ', len(comments_list))


if __name__ == "__main__":
    comments = ScrapyComments(API_KEY, VIDEO_ID)
    comments.get_comments()
    comments.save_to_csv()
