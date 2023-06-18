import json
import requests
import html
import os
from .Pipeline import BasePipeline

class YoutubePipeline(BasePipeline):
    all_comments_data = {}
    analysis_df = {}

    def __init__(self, videoID, description, all_comments_data=None):
        self.videoID = videoID
        self.max_top_down_length = 3000
        self.max_top_down_comments = 50
        self.max_bottom_up_length = 500
        self.max_bottom_up_comments = 10
        self.OpenAI_API_URL = "https://api.openai.com/v1/chat/completions"

        self.top_down_topics_prompt = """
Analyse the list of comments and give me the Top 20 topics (with their estimated counts of comments and sentiment). Share ONLY the topic list and do not share any other introductory/ header/ footer or explanation/ commentary text.

YouTube comment list:
        """
        self.top_down_topics_prompt = description + " " + self.top_down_topics_prompt

        self.top_down_topic_tagging_prompt = """
For each of the following YouTube comments, analyse and map them to one or more themes from the theme dictionary below. If none of the themes apply, then you can leave the theme mapping blank. Also analyse and find the sentiment of the comment (sentiment can only be positive or negative or neutral).

Format the results as a table, where the first column has the orginal comment's text (column title: "YouTube comment"), the second column has the list of the mapped themes (column title: "Detected themes"), and the third column has the detected sentiment (column title: "Sentiment").
        """

        self.top_down_topic_tagging_prompt = (
            description + " " + self.top_down_topic_tagging_prompt
        )

        self.not_present = {
            "top_down": [],
        }

        if not all_comments_data:
            all_comments_data = self.get_comments()
            self.store_comments(all_comments_data)
            self.print_comments_data()
            return

        self.store_comments(all_comments_data)

    def store_comments(self, all_comments_data):
        self.all_comments_data = all_comments_data
        self.all_comments = [
            " ".join(comment.split()[:200])
            for comment in self.all_comments_data["all_comments"]
        ]

    def get_comments(self):
        print("Fetching comments...")
        params = {
            "key": os.environ.get("YOUTUBE_API_KEY"),
            "videoId": self.videoID,
            "part": "snippet",
            "order": "relevance",
            "maxResults": 100,
        }

        x = requests.get(
            "https://www.googleapis.com/youtube/v3/commentThreads", params=params
        )

        data = json.loads(x.text)

        all_texts = []
        total_likes = 0
        total_replies = 0
        total_length = 0
        author_dict = {}
        comments_with_replies = 0

        for text in data["items"]:
            comment_text = html.unescape(
                text["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            )

            # if comment is too big skip
            tokenized_comment_length = self.count_tokens(comment_text)
            if tokenized_comment_length > self.max_bottom_up_length:
                continue
            all_texts.append(comment_text)

            # add statistics
            total_likes += int(
                text["snippet"]["topLevelComment"]["snippet"]["likeCount"]
            )
            total_replies += int(text["snippet"]["totalReplyCount"])
            if int(text["snippet"]["totalReplyCount"]) > 0:
                comments_with_replies += 1
            total_length += len(comment_text.split())

            # author info
            author_id = text["snippet"]["topLevelComment"]["snippet"][
                "authorChannelId"
            ]["value"]
            author_name = text["snippet"]["topLevelComment"]["snippet"][
                "authorDisplayName"
            ]

            if author_id in author_dict.keys():
                author_dict[author_id]["comments"].append(comment_text)
            else:
                author_dict[author_id] = {
                    "comments": [comment_text],
                    "name": author_name,
                }

        next_token = data["nextPageToken"]
        # next_token = False

        ct = 1
        while next_token:
            print("Comments fetched: ", len(all_texts))
            params = {
                "key": "AIzaSyAmrWaHTfZLh1B5UYFUlColWwxzegbRHFU",
                "videoId": self.videoID,
                "part": "snippet",
                "order": "relevance",
                "maxResults": 100,
                "pageToken": next_token,
            }

            x = requests.get(
                "https://www.googleapis.com/youtube/v3/commentThreads", params=params
            )

            data = json.loads(x.text)
            for text in data["items"]:
                comment_text = html.unescape(
                    text["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                )

                # if comment is too big skip
                tokenized_comment_length = self.count_tokens(comment_text)
                if tokenized_comment_length > self.max_bottom_up_length:
                    continue
                all_texts.append(comment_text)

                # count statistics
                total_likes += int(
                    text["snippet"]["topLevelComment"]["snippet"]["likeCount"]
                )
                total_replies += int(text["snippet"]["totalReplyCount"])
                if int(text["snippet"]["totalReplyCount"]) > 0:
                    comments_with_replies += 1
                total_length += len(comment_text.split())

                # author info
                author_id = text["snippet"]["topLevelComment"]["snippet"][
                    "authorChannelId"
                ]["value"]
                author_name = text["snippet"]["topLevelComment"]["snippet"][
                    "authorDisplayName"
                ]

                if author_id in author_dict.keys():
                    author_dict[author_id]["comments"].append(comment_text)
                else:
                    author_dict[author_id] = {
                        "comments": [comment_text],
                        "name": author_name,
                    }

            next_token = data.get("nextPageToken", None)
            ct += 1

        # final data
        all_comments_data = {
            "all_comments": all_texts,
            "number_of_comments": len(all_texts),
            "comments_with_replies": comments_with_replies,
            "total_likes": total_likes,
            "avg_likes": float(total_likes) / len(all_texts),
            "total_replies": total_replies,
            "avg_replies": float(total_replies) / len(all_texts),
            "avg_comment_length": float(total_length) / len(all_texts),
            "author_dict": author_dict,
        }
        return all_comments_data

    def print_comments_data(self):
        all_comments_data = self.all_comments_data
        print("Total comments: ", all_comments_data["number_of_comments"])
        print("Comments with replies: ", all_comments_data["comments_with_replies"])
        print("Total likes: ", all_comments_data["total_likes"])
        print("Average likes: ", all_comments_data["avg_likes"])
        print("Total replies: ", all_comments_data["total_replies"])
        print("Average replies: ", all_comments_data["avg_replies"])
        print("Average comment length: ", all_comments_data["avg_comment_length"])