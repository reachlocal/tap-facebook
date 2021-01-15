from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import sys
import os
import os.path
import simplejson as json
import singer
import requests

LOGGER = singer.get_logger()
MAX_PAGE_SIZE = 5000

class FacebookReportingService:

    def __init__(self, stream, schema, config):
        self.config = config
        self.stream = stream
        self.props = [(k, v) for k, v in schema["properties"].items()]
        self.api_url = 'https://graph.facebook.com/v9.0'
        self.access_token = config['accessToken']

        common_fields = 'campaign_id,campaign_name,account_id,account_name'
        campaign_fields = 'actions,action_values,unique_actions,clicks,impressions,reach,inline_link_clicks,spend,frequency,video_p100_watched_actions'
        ad_fields = 'adset_id,adset_name,ad_name,ad_id,actions,unique_actions,impressions,clicks,reach,inline_link_clicks,frequency,spend,video_p100_watched_actions,quality_ranking,engagement_rate_ranking,conversion_rate_ranking'
        default_actions = ['landing_page_view', 'post_reaction', 'video_view', 'link_click',
                'page_engagement', 'post_engagement', 'post', 'comment', 'like', 'action_reaction']
        self.schema_map = {
            'campaign_performance_report': {
                'level': 'campaign',
                'fields': f'{common_fields},{campaign_fields}',
                'params': {},
                'supported_actions': default_actions,
                'action_values': False
            },
            'campaign_by_placement_performance_report': {
                'level': 'campaign',
                'fields': f'{common_fields},{campaign_fields}',
                'params': {
                    'breakdowns': 'platform_position,publisher_platform,device_platform'
                },
                'supported_actions': default_actions,
                'action_values': False
            },
            'campaign_by_age_gender_performance_report': {
                'level': 'campaign',
                'fields': f'{common_fields},{campaign_fields}',
                'params': {
                    'breakdowns': 'age,gender'
                },
                'supported_actions': default_actions,
                'action_values': False
            },
            'campaign_by_impression_device_performance_report': {
                'level': 'campaign',
                'fields': f'{common_fields},{campaign_fields}',
                'params': {
                    'breakdowns': 'impression_device'
                },
                'supported_actions': default_actions,
                'action_values': False
            },
            'ad_performance_report': {
                'level': 'ad',
                'fields': f'{common_fields},{ad_fields}',
                'params': {},
                'supported_actions': default_actions,
                'action_values': False
            },
            'ad_by_placement_performance_report': {
                'level': 'ad',
                'fields': f'{common_fields},{ad_fields}',
                'params': {
                    'breakdowns': 'platform_position,publisher_platform,device_platform'
                },
                'supported_actions': default_actions,
                'action_values': False
            },
            'ad_by_age_gender_performance_report': {
                'level': 'ad',
                'fields': f'{common_fields},{ad_fields}',
                'params': {
                    'breakdowns': 'age,gender'
                },
                'supported_actions': default_actions,
                'action_values': False
            },
            'ad_by_impression_device_performance_report': {
                'level': 'ad',
                'fields': f'{common_fields},{ad_fields}',
                'params': {
                    'breakdowns': 'impression_device'
                },
                'supported_actions': default_actions,
                'action_values': False
            },
            'offline_conversion_performance_report': {
                'level': 'campaign',
                'fields': f'{common_fields},actions,action_values',
                'params': {},
                'supported_actions': ['offline_conversion.purchase'],
                'action_values': True
            },
            'offline_conversion_by_age_gender_performance_report': {
                'level': 'campaign',
                'fields': f'{common_fields},actions,action_values',
                'params': {
                    'breakdowns': 'age,gender'
                },
                'supported_actions': ['offline_conversion.purchase'],
                'action_values': True
            },
            'offline_conversion_by_impression_device_performance_report': {
                'level': 'campaign',
                'fields': f'{common_fields},actions,action_values',
                'params': {
                    'breakdowns': 'impression_device'
                },
                'supported_actions': ['offline_conversion.purchase'],
                'action_values': True
            }
        }
    
    def get_reports(self):
        account_ids = self.retrieve_account_ids()
        LOGGER.info('Retrieved account IDs')
        total = len(account_ids)

        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(lambda arg: self.retrieve_report_for_account(arg[1], arg[0], total), enumerate(account_ids))

    def retrieve_report_for_account(self, account, index, total):
        date_parts = self.config['dateRange'].split(',')
        reporting_params = {
            "access_token": self.access_token,
            "limit": MAX_PAGE_SIZE,
            "fields": self.schema_map[self.stream]['fields'],
            "time_range": json.dumps({
                'since': self.parse_date(date_parts[0]),
                'until': self.parse_date(date_parts[1])
            }),
            "time_increment": 1,
            "level": self.schema_map[self.stream]['level']
        }

        reporting_params.update(self.schema_map[self.stream]['params'])
        acc_report = list(map(lambda item: self.map_record(item), self.retrieve_paged_data(f'{self.api_url}/{account}/insights', reporting_params)))
        LOGGER.info(f'[{self.stream}] Retrieved {len(acc_report)} items for account {index + 1}/{total} ({account}) for {self.config["dateRange"]}')
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
            supported_actions = self.schema_map[self.stream]['supported_actions']
            for action in list(filter(lambda a: a['action_type'] in supported_actions, item['actions'])):
                obj[f'{action["action_type"].replace(".", "_")}s'] = int(action['value'])

        if 'unique_actions' in item:
            for action in list(filter(lambda a: a['action_type'] == 'link_click', item['unique_actions'])):
                obj['unique_link_clicks'] = int(action['value'])

        if self.schema_map[self.stream]['action_values'] and 'action_values' in item:
            for action in list(filter(lambda a: a['action_type'] == 'offline_conversion.purchase', item['action_values'])):
                obj['offline_conversion_purchase_value'] = float(action['value'])

        obj['report_date'] = item['date_start']
        if 'video_views_p100' in obj:
            obj['video_views_p100'] = int(item['video_p100_watched_actions'][0]['value']) if 'video_p100_watched_actions' in item else 0
        return obj

    def retrieve_account_ids(self):
        user_id = requests.get(f'{self.api_url}/me', { "access_token": self.access_token }).json()['id']

        account_params = {
            "access_token": self.access_token,
            "fields": "id",
            "limit": MAX_PAGE_SIZE
        }
        accounts = self.retrieve_paged_data(f'{self.api_url}/{user_id}/adaccounts', account_params)
        account_ids = list(map(lambda acc: acc['id'], accounts))
        return account_ids

    def retrieve_paged_data(self, url, params):
        result = []
        has_next_page = True
        next_page_token = ''

        try:
            while has_next_page:
                params['after'] = next_page_token
                raw_response = requests.get(url, params)
                if raw_response.status_code != 200:
                    if params['limit'] == MAX_PAGE_SIZE:
                        LOGGER.info(f'Request failed for {url}, retrying with a lower limit')
                        params['limit'] = 1000
                        return self.retrieve_paged_data(url, params)
                    else:
                        LOGGER.info(f'Request failed for {url}')
                        return []
                response = raw_response.json()
                result.extend(response['data'])
                has_next_page = 'next' in response['paging'] if 'paging' in response else False
                next_page_token = response['paging']['cursors']['after'] if 'paging' in response else ''
        except Exception as ex:
            LOGGER.info(ex)
        return result