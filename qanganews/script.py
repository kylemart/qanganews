from datetime import datetime
import json
import logging
import os
import re
import sys

from oauth2client.service_account import ServiceAccountCredentials
from plexapi.myplex import MyPlexAccount
import gspread
import numpy as np
import pandas as pd
import requests
import toml


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def clean_email(email):
    username, domain = email.lower().split('@')
    username = username.replace('.', '')
    cleaned = f'{username}@{domain}'
    return cleaned
    

def get_friends(config):
    token = config['plex']['token']
    account = MyPlexAccount(token)
    columns = ['email', 'cleaned_email']
    friends = [
        (user.email, clean_email(user.email))
        for user in account.users() 
        if user.friend
    ]
    frame = pd.DataFrame(friends, columns=columns)
    return frame
    

def get_submissions(config):   
    keyfile_dict = json.loads(config['sheet']['credentials'])
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(config['sheet']['key'])
    records = spreadsheet.sheet1.get_all_records()
    columns = {
        config['sheet']['timestamp-col']: 'ts',
        config['sheet']['email-col']: 'email',
        config['sheet']['cadence-col']: 'cadence'
    }
    frame = pd.DataFrame(records, columns=columns.keys())
    frame = frame.rename(columns=columns)
    frame.ts = pd.to_datetime(frame.ts)
    frame = frame.replace(r'^\s*$', np.nan, regex=True).dropna()
    frame['cleaned_email'] = frame.email.apply(clean_email)
    frame = frame[frame.groupby('cleaned_email').ts.transform(max) == frame.ts]
    frame = frame[['cleaned_email', 'cadence']]  
    return frame
    

def get_notifiers(config):
    columns = ['cadence', 'notifier_id']
    notifiers = config['tautulli']['notifiers']
    frame = pd.DataFrame(notifiers, columns=columns)
    return frame
    

def cook_dataframes(friends, submissions, notifiers):
    frame = friends.join(submissions.set_index('cleaned_email'), on='cleaned_email', how='left')
    frame.cadence = frame.cadence.replace(np.nan, notifiers.cadence[0])
    frame = frame.join(notifiers.set_index('cadence'), on='cadence', how='inner')
    frame = frame.groupby(['cadence', 'notifier_id']).email.apply(set)
    frame = frame.reset_index(name='emails')
    frame = frame[['notifier_id', 'emails']]
    frame = notifiers.merge(frame.set_index('notifier_id'), on='notifier_id', how='left')
    frame.emails = frame.emails.apply(lambda e: e if isinstance(e, set) else {}) 
    return frame
    

def update_notifiers(config, cooked):
    host = config['tautulli']['host']
    port = config['tautulli']['port']
    post_url = f'http://{host}:{port}/set_notifier_config'
    for _, row in cooked.iterrows():
        response = requests.post(post_url, data={
            'notifier_id': row.notifier_id,
            'agent_id': 10,
            'email_from': config['email']['username'],
            'email_from_name': 'Tautulli Newsletter',
            'email_bcc': row.emails,
            'email_html_support': 1,
            'email_tls': 1 if config['email']['tls'] else 0,
            'email_smtp_server': config['email']['server'],
            'email_smtp_port': config['email']['port'],
            'email_smtp_user': config['email']['username'],
            'email_smtp_password': config['email']['password'],
            'friendly_name': f'Newsletter Group - {row.cadence}'
        })
        logger.info(f'Updated notifier - notifier:"{row.cadence}", success:"{response.ok}"')
        logger.debug(f'Update details - emails:{row["emails"]}')
        

def main():
    config = None
    try:
        logger.info('Loading config file...')
        config = toml.load('config/config.toml')
    except FileNotFoundError:
        logger.error('Config file not found!')
        raise
    try:    
        logger.info('Beginning task(s)...')
        cooked = cook_dataframes(
            get_friends(config), 
            get_submissions(config),
            get_notifiers(config)
        )
        update_notifiers(config, cooked)
    except Exception:
        logger.error('Unexpected error. See stderr for info.')
        raise
    finally:
        logger.info('Completed tasks(s)')
        

if __name__ == '__main__':
    main()
