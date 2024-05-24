import praw
from transformers import pipeline
import datetime as dt
import pandas as pd

MAX_SEQUENCE_LENGTH = 512
MONTHS = ["December", "November", "October", "September", "August", "July", "June", "May", "April", "March", "February", "January"]
YEARS = ["2024", "2023"]

def get_reddit_comments():
    reddit_comments = pd.DataFrame(columns=['ID', 'LOAD_DT', 'BASE_DT', 'COMMENT_CONTENT', 'COMMENT_SENTIMENT'])
    pipe = pipeline("text-classification", model="mwkby/distilbert-base-uncased-sentiment-reddit-crypto")
    reddit = praw.Reddit(
        client_id="Q0r0aDnOshw7X7M4vHnzIw",
        client_secret="XkMtcZ_96Hw3DPH_aJw_ghbszu7jBg",
        user_agent="testscript by u/purotae",
    )
    i = 1
    subreddit = reddit.subreddit("wallstreetbets")
    for year in YEARS:
        for month in MONTHS:
            for day in range(31, 0, -1):
                if year == "2024" and month in ["May", "April", "March"] and day < 10:
                    day = str(day)
                elif day < 10:
                    day = str(day).zfill(2)
                else:
                    day = str(day)
                fixed_title = 'Daily Discussion Thread for {} {}, {}'.format(month, day, year)
                search_results = list(subreddit.search(query=fixed_title, limit=1))

                if search_results:
                    submission = search_results[0]
                else:
                    continue

                if submission.title != fixed_title:
                    continue
                date = dt.date.fromtimestamp(submission.created_utc)
                submission.comments.replace_more(limit=0)
                for comment in submission.comments.list()[:300]:
                    raw_content = comment.body.replace('\n', ' ').replace('\r', ' ').replace('|', ' ').replace(',', '')
                    list_content = strip_text(raw_content, MAX_SEQUENCE_LENGTH)
                    sentiment = judge_setiment(pipe(list_content))
                    reddit_comments.loc[i] = [i, dt.datetime.now(), date, raw_content.lower(), sentiment]
                    i += 1
                print(reddit_comments)
        reddit_comments.to_csv('WH_REDDIT_COMMENT.csv', index=False, mode='a')
        reddit_comments = reddit_comments.iloc[0:0]


def strip_text(text, max_length=MAX_SEQUENCE_LENGTH):
    results = []
    for i in range(0, len(text), max_length):
        results.append(text[i:i + max_length])
    return results

def judge_setiment(sentiments):
    sum = 0
    for sentiment in sentiments:
        sum += sentiment['score']
    mean = sum / len(sentiment)
    if mean >= 0.5:
        return 'positive'
    elif mean < 0.5:
        return 'negative'

def main():
    get_reddit_comments()

if __name__ == "__main__":
    main()