import os
import time
from dotenv import load_dotenv
load_dotenv()

import gspread
import feedparser

with open('./service_credentials.json', 'w') as file:
    file.write(os.getenv('GOOGLE_SERVICE_CREDENTIALS'))

gc = gspread.service_account('./service_credentials.json')
sheet = gc.open_by_key(os.getenv("SHEET_ID"))


from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()

@sched.scheduled_job('interval', minutes=1)
def timed_job():
    print('started loop', time.time())
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

    for alert_item in table_data:
        rss_link = alert_item['rss_link']
        company_name = alert_item['company_name']

        crm_id = list(filter(lambda d: d['company_name'].lower() == company_name.lower(), table_data))[0]['crm_id']

        feed = feedparser.parse(rss_link)

        for item in feed['entries']:
            if item['id'] not in news_ids:
                print('added new item with id ' + item['id'])
                news_sheet.append_row(
                    [item['id'], item['title'], item['link'], item['published'], company_name, crm_id])

sched.start()
