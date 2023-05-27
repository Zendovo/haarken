import tiktoken
import json
import requests
import html
import pandas as pd
import asyncio
import aiohttp

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
stopwords_en = stopwords.words('english')


class BasePipeline:
    def count_tokens(self, text):
        encoding = tiktoken.get_encoding("cl100k_base")
        num_tokens = len(encoding.encode(text))
        return num_tokens

    async def async_gpt_completion_call(self, session, prompt) -> str:
        params = {
            "model": "gpt-3.5-turbo",
            "messages": [
                {"role": "user", "content": prompt}
            ],
        }
        response = await session.request(
            method="post",
            url=self.OpenAI_API_URL,
            json=params,
            headers={
                "Authorization": f"Bearer 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"
            },
        )
        try:
            t = await response.json()
            return { 'error': False, 'content': t["choices"][0]["message"]["content"] }
        except Exception as e:
            return { 'error': e }


class YoutubePipeline(BasePipeline):
    all_comments_data = {}

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
            "key": "AIzaSyAmrWaHTfZLh1B5UYFUlColWwxzegbRHFU",
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
        next_token = False

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

    def parse_comments(self, comments):
        all_str = ""
        for i, text in enumerate(comments):
            all_str += str(i + 1) + " " + text + "\n"
        return all_str

    async def get_top_down_topics(self, session, parsed_comments):
        prompt = self.top_down_topics_prompt + "\n" + parsed_comments
        print('test')
        data = await self.async_gpt_completion_call(session, prompt)
        print(data)
        if data['error']:
            raise requests.RequestException
        
        top_down_topics = data['content']

        topics_without_header_without_metadata = []
        for topic_text in top_down_topics.split("\n"):
            try:
                topics_without_header_without_metadata.append(
                    topic_text.split("(")[0]
                    .strip()
                    .split("-")[0]
                    .strip()
                    .split(".")[1]
                    .strip()
                )
            except:
                pass
        top_down_topics = topics_without_header_without_metadata
        return top_down_topics

    async def get_top_down_topics_tagging(
        self, session, parsed_comments, top_down_topics
    ):
        print('here')
        prompt = (
            self.top_down_topic_tagging_prompt
            + "\n"
            + "Theme dictionary: "
            + "\n"
            + "\n".join(top_down_topics)
            + "\n"
            + "YouTube comment list:"
            + "\n"
            + parsed_comments
        )
        data = await self.async_gpt_completion_call(session, prompt)
        if data['error']:
            raise requests.RequestException
        
        print('got it')
        completion = data['content']
        return completion

    def adjust_token_limit(self, start_idx, end_idx, max_tokens):
        parsed_comments = self.parse_comments(self.all_comments[start_idx:end_idx])

        num_tokens = self.count_tokens(parsed_comments)
        while num_tokens > max_tokens:
            print("Decreasing end idx from: ", str(end_idx))
            end_idx = end_idx - 10
            print("New end idx: ", str(end_idx))

            parsed_comments = self.parse_comments(self.all_comments[start_idx:end_idx])
            num_tokens = self.count_tokens(parsed_comments)

        return {"parsed_comments": parsed_comments, "end_idx": end_idx}

    async def get_analyses(self):
        start_idx = 0
        end_idx = min(start_idx + self.max_top_down_comments, len(self.all_comments))

        self.analysis_df = {
            "comments": [],
            "Top Down Topics": [],
            "Top Down Topics Tagged": [],
        }

        parsed_comments_list = []

        once_equal_break = 0
        while end_idx < len(self.all_comments):
            if once_equal_break:
                break

            if end_idx == len(self.all_comments):
                once_equal_break = 1

            print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
            print("Top Down topics...")

            _temp = self.adjust_token_limit(
                start_idx, end_idx, self.max_top_down_length
            )
            parsed_comments = _temp["parsed_comments"]
            end_idx = _temp["end_idx"]

            self.analysis_df["comments"].append(parsed_comments)
            print(
                "Within limit from start_idx: ",
                str(start_idx) + " to end_idx: ",
                str(end_idx),
            )

            # Fetch topics from start_idx to end_idx
            parsed_comments_list.append((start_idx, end_idx, parsed_comments))

            start_idx = end_idx
            end_idx = min(
                start_idx + self.max_top_down_comments, len(self.all_comments)
            )

        async with aiohttp.ClientSession() as session:
            async_responses = [
                self.top_down_topics_tagging(session, pair[0], pair[1], pair[2])
                for pair in parsed_comments_list
            ]
            all_topics = await asyncio.gather(*async_responses)

        # store topics
        self.analysis_df["Top Down Topics"].extend(all_topics)

        return self.analysis_df

    async def top_down_topics_tagging(
        self, session, start_idx, end_idx, parsed_comments
    ):
        print("----------------------------")
        print("Top Down and bottom up topic tagging...")

        mini_end_idx = min(start_idx + self.max_bottom_up_comments, end_idx)

        comments_list = []

        top_down_topics = await self.get_top_down_topics(session, parsed_comments)
        self.analysis_df["Top Down Topics"].extend(top_down_topics)

        # Topic tagging
        mini_once_equal_break = 0
        while mini_end_idx <= end_idx:
            if mini_once_equal_break:
                break

            if mini_end_idx == end_idx:
                mini_once_equal_break = 1

            print(
                "Topics tagging from start idx: ",
                str(start_idx),
                " to end idx: ",
                str(mini_end_idx),
            )
            _temp = self.adjust_token_limit(
                start_idx, mini_end_idx, self.max_bottom_up_length
            )
            parsed_comments = _temp["parsed_comments"]
            mini_end_idx = _temp["end_idx"]

            print(
                "Within limit from start_idx: ",
                str(start_idx) + " to mini end_idx: ",
                str(mini_end_idx),
            )

            comments_list.append(parsed_comments)

            start_idx = mini_end_idx
            mini_end_idx = min(start_idx + self.max_bottom_up_comments, end_idx)

        async_responses = [
            self.get_top_down_topics_tagging(session, comment, top_down_topics)
            for comment in comments_list
        ]
        all_top_down_tagging = await asyncio.gather(*async_responses)

        all_top_down_tagging = "\n".join(all_top_down_tagging)
        self.analysis_df["Top Down Topics Tagged"].append(all_top_down_tagging)

    def parse_analyses(self):
        print("Parsing top down tagging")
        split_tags_td = []

        positive = 0
        negative = 0
        neutral = 0
        comment_sentiment_dict = []

        for tag_m in self.analysis_df["Top Down Topics Tagged"]:
            for tag in tag_m.split("\n"):
                if "youtube comment" in tag.lower() or (
                    "mapped themes" in tag.lower() and "comment text" in tag.lower()
                ):
                    continue
                if "---" in tag:
                    continue
                if not tag:
                    continue

                try:
                    sentiment = tag.split("|")[3].strip().lower()
                except:
                    pass

                try:
                    category = tag.split("|")[4].strip().lower()
                except:
                    category = "NA"

                if sentiment == "positive":
                    positive += 1
                elif sentiment == "negative":
                    negative += 1
                if sentiment == "neutral":
                    neutral += 1

                comment_sentiment_dict.append(
                    {"Sentiment": sentiment, "Comment": tag, "Category": category}
                )
                split_tags_td.append(tag)

        comment_sentiment_df = pd.DataFrame(data=comment_sentiment_dict)
        comment_sentiment_df.to_csv("Comments_Sentiment_" + str(self.videoID) + ".csv")

        print("Comments' sentiments:")
        print(
            "Positive: ",
            str(float(positive) / self.all_comments_data["number_of_comments"]),
        )
        print(
            "Negative: ",
            str(float(negative) / self.all_comments_data["number_of_comments"]),
        )
        print(
            "Neutral: ",
            str(float(neutral) / self.all_comments_data["number_of_comments"]),
        )

        print("Positive: ", str(float(positive)))
        print("Negative: ", str(float(negative)))
        print("Neutral: ", str(float(neutral)))

        top_down_dict = dict(
            {k: [] for k in self.analysis_df["Top Down Topics"] if k != ""}
        )
        for comment in split_tags_td:
            flag = 0
            try:
                comment_topics = " ".join(
                    [
                        word
                        for word in comment.split("|")[2].strip().split()
                        if word not in stopwords.words("english")
                    ]
                )
            except:
                continue
            for topic in top_down_dict.keys():
                # topic_without_stopwords = ' '.join([word for word in topic.split() if word not in stopwords.words('english')])
                # if topic_without_stopwords.lower() in comment_topics.lower():
                if topic.lower() in comment.lower():
                    # if topic.lower()[:13] in comment.lower():
                    top_down_dict[topic].append(comment)
                    flag = 1
            if not flag:
                self.not_present["top_down"].append(comment)

        # print ("Parsing bottom up tagging")
        # split_tags_bu = []

        # for tag_m in self.analysis_df["Bottom Up Topics Tagged"]:
        #   for tag in tag_m.split("\n"):
        #     if 'comment' in tag.lower() and 'themes' in tag.lower() and 'sentiments' in tag.lower():
        #       continue
        #     if '---' in tag:
        #       continue
        #     split_tags_bu.append(tag)
        # bottom_up_topics = []

        # for bu_tag in split_tags_bu:
        #   try:
        #     topics = bu_tag.split('|')[2].strip()
        #   except:
        #     pass
        #   bottom_up_topics.extend([topic.lower().strip() for topic in topics.split(',')])

        # bottom_up_topics = set(bottom_up_topics)
        # self.analysis_df["Bottom Up Topics"] = bottom_up_topics
        # bottom_up_dict = dict({k:[] for k in self.analysis_df["Bottom Up Topics"]})
        # for comment in split_tags_bu:
        #   flag = 0
        #   for topic in bottom_up_dict.keys():
        #     try:
        #       if topic.lower() in comment.split('|')[2].lower():
        #         bottom_up_dict[topic].append(comment)
        #         flag = 1
        #     except:
        #       print ('Bad comment: ', comment)
        #   if not flag:
        #     self.not_present["bottom_up"].append(comment)

        # self.bottom_up_dict = bottom_up_dict
        self.top_down_dict = top_down_dict

        # combined_dict = {}

        # for k, v in self.top_down_dict.items():
        #   if k in combined_dict.keys():
        #     combined_dict[k].extend(v)
        #   else:
        #     combined_dict[k] = v

        # for k, v in self.bottom_up_dict.items():
        #   if k in combined_dict.keys():
        #     combined_dict[k].extend(v)
        #   else:
        #     combined_dict[k] = v

        # self.combined_dict = combined_dict

        top_down_df = []

        for k in top_down_dict.keys():
            for v in top_down_dict[k]:
                top_down_df.append({"Topic": k, "Comment": v})

        # bottom_up_df = []

        # for k in bottom_up_dict.keys():
        #   if k == '':
        #     continue
        #   for v in bottom_up_dict[k]:
        #     bottom_up_df.append({"Topic": k, "Comment": v})

        # combined_df = []

        # for k in combined_dict.keys():
        #   if k == '':
        #     continue
        #   for v in combined_dict[k]:
        #     combined_df.append({"Topic": k, "Comment": v})

        td_df = pd.DataFrame(data=top_down_df)
        # bu_df = pd.DataFrame(data=bottom_up_df)
        # combined_df = pd.DataFrame(data=combined_df)

        td_df.to_csv("Top_Down_Raw_" + self.videoID + ".csv")
        # bu_df.to_csv('Bottom_Up_Raw_'+self.videoID+'.csv')
        # combined_df.to_csv('Combined_Raw_'+self.videoID+'.csv')

        top_down_stats = []

        for k in top_down_dict.keys():
            top_down_stats.append({"Topic": k, "Comments": len(top_down_dict[k])})

        # bottom_up_stats = []

        # for k in bottom_up_dict.keys():
        #   if k == '':
        #     continue
        #   bottom_up_stats.append({"Topic": k, "Comments": len(bottom_up_dict[k])})

        # combined_stats = []

        # for k in combined_dict.keys():
        #   if k == '':
        #     continue
        #   combined_stats.append({"Topic": k, "Comments": len(combined_dict[k])})

        td_stats = pd.DataFrame(data=top_down_stats)
        # bu_stats = pd.DataFrame(data=bottom_up_stats)
        # combined_stats = pd.DataFrame(data=combined_stats)

        # td_stats.to_csv('Top_Down_Stats_'+self.videoID+'.csv')
        # bu_stats.to_csv('Bottom_Up_Stats_'+self.videoID+'.csv')
        # combined_stats.to_csv('Combined_Stats_'+self.videoID+'.csv')

        sentiment_dict = []
        for k, v in top_down_dict.items():
            # print ('Topic: ', k)
            mini_pos = 0
            mini_neg = 0
            mini_neu = 0
            if len(v) < 3:
                continue
            for comment in v:
                try:
                    sentiment = comment.split("|")[3].strip()
                except:
                    continue
                if sentiment.lower() == "positive":
                    mini_pos += 1
                elif sentiment.lower() == "negative":
                    mini_neg += 1
                elif sentiment.lower() == "neutral":
                    mini_neu += 1
                else:
                    print("None")
                    print(comment)
            if mini_pos > mini_neg and mini_pos > mini_neu:
                sentiment = "Positive"
            elif mini_neg > mini_pos and mini_neg > mini_neu:
                sentiment = "Negative"
            elif mini_neu > mini_pos and mini_neu > mini_neg:
                sentiment = "Neutral"
                # sentiment_dict.append({"Topic": k, "Sentiment": 'Neutral'})
            else:
                if mini_pos == mini_neg:
                    sentiment = "Positive, Negative"
                if mini_pos == mini_neu:
                    sentiment = "Positive, Neutral"
                if mini_neu == mini_neg:
                    sentiment = "Neutral, Negative"
                print("None largest")
                print("Positive: ", str(mini_pos))
                print("Negative: ", str(mini_neg))
                print("Neutral: ", str(mini_neu))
                print("Comment: ", comment.split("|"))
                print("Sentiment:", comment.split("|")[3].strip())
                print("------------------------------")
            sentiment_dict.append(
                {"Topic": k, "Sentiment": sentiment, "Comments": len(v)}
            )

        topics_sentiment_df = pd.DataFrame(data=sentiment_dict)
        topics_sentiment_df.to_csv(
            "Topics_Sentiment_Count_" + str(self.videoID) + ".csv"
        )
