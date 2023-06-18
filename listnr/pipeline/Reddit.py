import json
import requests
import html
import os
import time
from .Pipeline import BasePipeline

class RedditPipeline(BasePipeline):
    all_comments_data = {}
    analysis_df = {}

    def __init__(self, searchTerm, description, all_comments_data=None):
        self.searchTerm = searchTerm
        self.max_top_down_length = 3000
        self.max_top_down_comments = 50
        self.max_bottom_up_length = 500
        self.max_bottom_up_comments = 10
        self.OpenAI_API_URL = "https://api.openai.com/v1/chat/completions"

        self.top_down_topics_prompt = """
Analyse the list of comments and give me the Top 20 topics (with their estimated counts of comments and sentiment). Share ONLY the topic list and do not share any other introductory/ header/ footer or explanation/ commentary text.

Reddit post list:
        """
        self.top_down_topics_prompt = description + " " + self.top_down_topics_prompt

        self.top_down_topic_tagging_prompt = """
For each of the following Reddit post, analyse and map them to one or more themes from the theme dictionary below. If none of the themes apply, then you can leave the theme mapping blank. Also analyse and find the sentiment of the comment (sentiment can only be positive or negative or neutral).

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
        print("Fetching posts...")

        params = {
            "q": self.searchTerm,
            "t": "week",
            "sort": "top",
            "limit": 100,
        }

        auth = requests.auth.HTTPBasicAuth('PXpdpIZcC-dB0lix0Jtrag', 'PWQKbG8dIGr5AS6dZi05J53T3JrWYw')

        x = requests.get(
            "https://www.reddit.com/search.json", params=params, auth=auth, headers = {'User-agent': 'your bot 0.1'}
        )
        print(x.headers)

        data = json.loads(x.text)
        if "data" in data:
            data = data["data"]
        else:
            raise Exception(data)

        all_texts = []
        total_ups = 0
        total_replies = 0
        total_length = 0
        author_dict = {}
        comments_with_replies = 0

        for post in data["children"]:
            title = html.unescape(
                f'{post["data"]["subreddit"]} {post["data"]["title"]}'
            )

            # if comment is too big skip
            tokenized_comment_length = self.count_tokens(title)
            if tokenized_comment_length > self.max_bottom_up_length:
                continue
            all_texts.append(title)

            # add statistics
            total_ups += int(
                post["data"]["ups"]
            )
            total_replies += int(post["data"]["num_comments"])
            total_length += len(title.split())

            # author info
            author_id = post["data"]["author_fullname"]
            author_name = post["data"]["author_fullname"]

            if author_id in author_dict.keys():
                author_dict[author_id]["comments"].append(title)
            else:
                author_dict[author_id] = {
                    "comments": [title],
                    "name": author_name,
                }

        time.sleep(3)
        next_token = data["after"]
        # next_token = False

        ct = 1
        while False:
            time.sleep(3)
            if ct > 5:
                break
            print("Comments fetched: ", len(all_texts))
            params = {
                "q": self.searchTerm,
                "t": "week",
                "sort": "top",
                "limit": 100,
                "after": next_token
            }

            x = requests.get(
                "https://www.reddit.com/search.json", params=params, auth=auth, headers = {'User-agent': 'your bot 0.1'}
            )

            data = json.loads(x.text)
            if "data" in data:
                data = data["data"]
            else:
                raise Exception(data)
            for post in data["children"]:
                title = html.unescape(
                    f'{post["data"]["subreddit"]} {post["data"]["title"]}'
                )

                # if comment is too big skip
                tokenized_comment_length = self.count_tokens(title)
                if tokenized_comment_length > self.max_bottom_up_length:
                    continue
                all_texts.append(title)

                # add statistics
                total_ups += int(
                    post["data"]["ups"]
                )
                total_replies += int(post["data"]["num_comments"])
                total_length += len(title.split())

                # author info
                author_id = post["data"]["author_fullname"]
                author_name = post["data"]["author_fullname"]

                if author_id in author_dict.keys():
                    author_dict[author_id]["comments"].append(title)
                else:
                    author_dict[author_id] = {
                        "comments": [title],
                        "name": author_name,
                    }

            next_token = data.get("after", None)
            ct += 1

        # final data
        all_comments_data = {
            "all_comments": all_texts,
            "number_of_comments": len(all_texts),
            "comments_with_replies": comments_with_replies,
            "total_likes": total_ups,
            "avg_likes": float(total_ups) / len(all_texts),
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