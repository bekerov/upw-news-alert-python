import os
import sched, time
from dotenv import load_dotenv
load_dotenv()

import gspread
import feedparser
from google_alerts import GoogleAlerts

print('File started')

with open('./service_credentials.json', 'w') as file:
    file.write(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))

gc = gspread.service_account('./service_credentials.json')
sheet = gc.open_by_key(os.getenv("SHEET_ID"))

s = sched.scheduler(time.time, time.sleep)


def load_news(sc):
    news_sheet = sheet.get_worksheet(0)

    companies_sheet = sheet.get_worksheet(1)

    news_ids = news_sheet.col_values(1)

    list_of_lists = companies_sheet.get_all_values()

    table_data = []

    for row in list_of_lists[1:]:
        dict = {}
        for idx, value in enumerate(row):
            dict[list_of_lists[0][idx]] = value
        table_data.append(dict)

    ga = GoogleAlerts(os.getenv("GOOGLE_EMAIL"), os.getenv("GOOGLE_PASSWORD"))
    ga.authenticate()

    alerts_list = ga.list()

    for alert_item in alerts_list:
        rss_link = alert_item['rss_link']
        company_name = alert_item['term']

        crm_id = list(filter(lambda d: d['company_name'].lower() == company_name.lower(), table_data))[0]['crm_id']

        feed = feedparser.parse(rss_link)

        for item in feed['entries']:
            if item['id'] not in news_ids:
                print('added new item with id ' + item['id'])
                news_sheet.append_row([item['id'], item['title'], item['link'], item['published'], company_name, crm_id])

    s.enter(60, 1, load_news, (sc,))


s.enter(60, 1, load_news, (s,))
s.run()
