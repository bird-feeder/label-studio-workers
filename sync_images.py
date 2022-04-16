#!/usr/bin/env python
# coding: utf-8

import gzip
import os
import signal
import sys
import time

import ray
import requests
import schedule
from dotenv import load_dotenv
from loguru import logger
from pymongo.errors import DuplicateKeyError
from tqdm import tqdm

from mongodb_helper import get_tasks_from_mongodb, mongodb_db


def keyboard_interrupt_handler(sig, _):
    print(f'KeyboardInterrupt (ID: {sig}) has been caught...')
    ray.shutdown()
    print('Terminating the session gracefully...')
    sys.exit(1)


@ray.remote
def img_url_to_binary(x):
    return {
        '_id': x['_id'],
        'file_name': x['data']['_image'].replace('https://srv.aibird.me/', ''),
        'image': gzip.compress(requests.get(x['data']['_image']).content)
    }


def sync_images():
    def insert_image(d):
        try:
            db.images.insert_one(d)
        except DuplicateKeyError:
            db.images.delete_one({'_id': d['_id']})
            db.images.insert_one(d)

    db = mongodb_db(os.environ['LOCAL_DB_CONNECTION_STRING'])
    main_db = mongodb_db(os.environ['DB_CONNECTION_STRING'])

    existing_ids = db.images.find().distinct('_id')
    projects_id = os.environ['PROJECTS_ID'].split(',')
    data = sum([
        get_tasks_from_mongodb(main_db, project_id, dump=False, json_min=False)
        for project_id in projects_id
    ], [])
    
    data = [x for x in data if x['_id'] not in existing_ids]

    futures = []
    for x in data:
        futures.append(img_url_to_binary.remote(x))

    results = []
    for future in tqdm(futures):
        insert_image(ray.get(future))


if __name__ == '__main__':
    load_dotenv()
    logger.add('logs.log')
    signal.signal(signal.SIGINT, keyboard_interrupt_handler)

    sync_images()  # run once before schedule
    schedule.every(6).hours.do(sync_images)

    while True:
        schedule.run_pending()
        time.sleep(1)
