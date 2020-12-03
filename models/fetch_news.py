import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime
# from firestore_test import db

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

cred = credentials.Certificate("./newsapi-92d9f-firebase-adminsdk-l8vje-743a14bb4e.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

def get_data(url):
    print(url,datetime.now())
    x = requests.get(url)
    soup = BeautifulSoup(x.text, 'html.parser')
    tr1 = soup.find("table")
    time.sleep(1)
    return tr1.text

def read_rss(url):
    x = requests.get(url)
    doc = BeautifulSoup(x.text, 'xml')

    # return {"items": [{'pubDate': item.find('pubDate').getText(), 'link': item.find('link').getText(),
    #                    'urlid': item.find('link').getText().split('=')[-1], 'title': item.find('title').getText(),
    #                    'summary': item.find('description').getText()} for item in doc.findAll('item')]}
    return {"items": [{'link': item.find('link').getText(), 'urlid': item.find('link').getText().split('=')[-1],
                       'title': item.find('title').getText(), 'summary': item.find('description').getText()} for item in
                      doc.findAll('item')]}
    # return {"items":[{'link': item.find('link').getText(), 'title': item.find('title').getText(),'summary': item.find('description').getText() }  for item in doc.findAll('item')]}


rss_feed_url = {
                'State': 'https://newsonair.gov.in/State_rss.aspx',
                "National": "https://newsonair.gov.in/National_rss.aspx",
                "Sports": "https://newsonair.gov.in/Sports_rss.aspx",
#                "business":"https://newsonair.gov.in/business_rss.aspx",
               "Top": "https://newsonair.gov.in/Top_rss.aspx"
}

for newsType in rss_feed_url.keys():
    print(newsType)
    #     feed = feedparser.parse( rss_feed_url[newsType] )
    feed = read_rss(rss_feed_url[newsType])
    id_list = [i["urlid"] for i in feed["items"]]
    comparision_list = [{i["urlid"]:i["title"]} for i in feed["items"]]

    if len(feed["items"]) % 10 == 0:
        iter_range = len(feed["items"]) // 10
    else:
        iter_range = len(feed["items"]) // 10 + 1

    for l in range(iter_range):
        #     print(l)
        doc = db.collection(newsType).where('urlid', 'in', id_list[l * 10:(l * 10) + 10]);
        docs = doc.stream()
        common_news = {}
        for i in docs:
            result = i.to_dict()
            common_news[result["urlid"]] = result["title1"]

        for k, j in common_news.items():
            c = 0
            for i in feed["items"]:
                if i['urlid'] == k:
                    feed["items"].pop(c)
                c += 1

    if len(feed["items"]) > 0:
        print('Start Time', datetime.now())
        result = [{'url': i['link'], 'title1': i['title'], 'summary': i['summary'],
                   'content': get_data(i['link']), 'newsType': newsType}
                  for i in feed["items"]]
        print('End Time', datetime.now())

        for i in range(len(result)):
            timestring = result[i]['content'].split('\n')[4]
            inttimestamp = datetime.strptime(timestring, '%b %d, %Y, %I:%M%p')
            # print(t)
            # result[i]["utctime"] = datetime.strptime(timestring, '%b %d, %Y, %I:%M%p')
            result[i]["newstime"] = datetime.strptime(timestring, '%b %d, %Y, %I:%M%p')
            # result[i]["pubDate"] = datetime.strptime(result[i]["pubDate"].replace("GMT", "").split(",")[-1].strip(),
            #                                          '%d %b %Y %H:%M:%S')
            result[i]["inttimestamp"] = int(inttimestamp.timestamp() * 1000)
            result[i]["title2"] = result[i]['content'].split('\n')[6]
            result[i]["allContent"] = ('\n'.join(result[i]['content'].split('\n')[11:])).strip()
            result[i]['insertedOn'] = datetime.now()
            result[i]['urlid'] = result[i]['url'].split('=')[-1]
            del result[i]['content']
            doc = db.collection(newsType).add(result[i])

    else:
        print("No news Articles found")
    # titlename = newsType
    time.sleep(10)

    print("\n")