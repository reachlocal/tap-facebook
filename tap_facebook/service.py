from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import sys
import os
import os.path
import simplejson as json
import singer
import requests

LOGGER = singer.get_logger()

class FacebookReportingService:

    def __init__(self, stream, schema, config):
        self.config = config
        self.stream = stream
        self.props = [(k, v) for k, v in schema["properties"].items()]
        self.api_url = 'https://graph.facebook.com/v9.0'
        self.access_token = config['accessToken']
    
    def get_reports(self):
        LOGGER.info('Getting reports')
        account_ids = self.retrieve_account_ids()
        for index, account in enumerate(account_ids):
            LOGGER.info(f'Retrieving reporting data for account {index + 1}/{len(account_ids)} ({account})')
            self.retrieve_report_for_account(account)


    def retrieve_report_for_account(self, account):
        date_parts = self.config['dateRange'].split(',')
        reporting_params = {
            "access_token": self.access_token,
            "limit": 5000,
            "fields": "actions,action_values,unique_actions,clicks,impressions,reach,inline_link_clicks,spend,frequency,video_p100_watched_actions,campaign_id,campaign_name,account_id,account_name",
            "time_range": json.dumps({
                'since': self.parse_date(date_parts[0]),
                'until': self.parse_date(date_parts[1])
            }),
            "time_increment": 1,
            "level": "campaign"
        }
        acc_report = list(map(lambda item: self.map_record(item), self.retrieve_paged_data(f'{self.api_url}/{account}/insights', reporting_params)))
        LOGGER.info(f'Retrieved {len(acc_report)} items')
        singer.write_records(self.stream, acc_report)

    def parse_date(self, date_str):
        date = datetime.strptime(date_str, "%Y%m%d").date()
        return date.strftime("%Y-%m-%d")

    def map_record(self, item):
        obj = {}
        for prop in self.props:
            key = prop[0]
            prop_type = prop[1]['type']

            if key in item:
                value = ''
                if prop_type == 'integer':
                    value = int(item[key])
                elif prop_type== 'number':
                    value = float(item[key])
                else:
                    value = item[key]
                obj[key] = value
            else:
                obj[key] = 0 if prop_type in ['integer', 'number'] else ''

        if 'actions' in item:
            supported_actions = ['video_view', 'link_click', 'page_engagement', 'post_engagement']
            for action in list(filter(lambda a: a['action_type'] in supported_actions, item['actions'])):
                obj[f'{action["action_type"]}s'] = int(action['value'])
        obj['report_date'] = item['date_start']
        obj['video_views_p100'] = int(item['video_p100_watched_actions'][0]['value']) if 'video_p100_watched_actions' in item else 0
        return obj

    def retrieve_account_ids(self):
        user_id = requests.get(f'{self.api_url}/me', { "access_token": self.access_token }).json()['id']

        account_params = {
            "access_token": self.access_token,
            "fields": "id",
            "limit": 5000
        }
        accounts = self.retrieve_paged_data(f'{self.api_url}/{user_id}/adaccounts', account_params)
        account_ids = list(map(lambda acc: acc['id'], accounts))[:10]
        return account_ids

    def retrieve_paged_data(self, url, params):
        result = []
        has_next_page = True
        next_page_token = ''
        while has_next_page:
            params['after'] = next_page_token
            raw_response = requests.get(url, params)
            if raw_response.status_code != 200:
                LOGGER.info('Request failed')
                return []
            response = raw_response.json()
            result.extend(response['data'])
            has_next_page = 'next' in response['paging'] if 'paging' in response else False
            next_page_token = response['paging']['cursors']['after'] if 'paging' in response else ''
        return result