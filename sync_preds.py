#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import signal
import sys
import traceback

import ray
import requests
from dotenv import load_dotenv
from loguru import logger
from tqdm import tqdm

from mongodb_helper import mongodb_db, get_tasks_from_mongodb


def keyboard_interrupt_handler(sig, _):
    logger.warning(f'KeyboardInterrupt (ID: {sig}) has been caught...')
    ray.shutdown()
    logger.warning('Terminating the session gracefully...')
    sys.exit(1)


def make_headers():
    headers = requests.structures.CaseInsensitiveDict()  # noqa
    headers['Content-type'] = 'application/json'
    headers['Authorization'] = f'Token {os.environ["TOKEN"]}'
    return headers


@ray.remote
def get_pred_details(pred_id):
    try:
        headers = make_headers()
        url = f'{os.environ["LS_HOST"]}/api/predictions/{pred_id}/'
        resp = requests.get(url, headers=headers)
        return resp.json()
    except Exception:  # temp debug
        print('>>>>>>>>>>>>>>>>>>>> Unexpected exception')  # temp debug
        print(traceback.format_exc())  # temp debug
        print('<<<<<<<<<<<<<<<<<<<<')  # temp debug


def get_project_pred_ids(db, project_id):

    all_tasks = {}
    tasks = get_tasks_from_mongodb(db, project_id, json_min=False, dump=False)
    existing_ids = db[f'project_{project_id}_preds'].find().distinct('_id')

    all_pred_ids = []
    for task in tasks:
        for pred_id in task['predictions']:
            if pred_id not in existing_ids:
                all_pred_ids.append(pred_id)
    return all_pred_ids


def process_preds(db, projects_id):
    projects_id = projects_id.split(',')

    for project_id in tqdm(projects_id, desc='Projects'):

        prediction_ids = get_project_pred_ids(db, project_id)
        if not prediction_ids:
            logger.debug(
                f'All predictions in project {project_id} are up-to-date')
            continue

        futures = []
        for pred_id in prediction_ids:
            futures.append(get_pred_details.remote(pred_id))

        for future in tqdm(futures, desc='Futures'):
            try:
                result = ray.get(future)
            except Exception as e:  # temp debug
                ray.cancel(future)
                logger.error(
                    '>>>>>>>>>>>>>>>>>>>> Unexpected exception')  # temp debug
                logger.error(traceback.format_exc())  # temp debug
                logger.error('<<<<<<<<<<<<<<<<<<<<')  # temp debug
                continue
            if isinstance(result, dict):
                result.update({'_id': result['id']})
                db[f'project_{project_id}_preds'].insert_one(result)
            else:
                logger.error('Result is not instance of dict!')  # temp debug
                logger.error(f'Result: {result}')  # temp debug
    return


def opts():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--projects-id',
                        help='Comma-seperated projects ID',
                        type=str,
                        default=os.environ['PROJECTS_ID'])
    return parser.parse_args()


def sync_preds():
    db = mongodb_db(os.environ['DB_CONNECTION_STRING'])
    process_preds(db, args.projects_id)
    return


if __name__ == '__main__':
    load_dotenv()
    signal.signal(signal.SIGINT, keyboard_interrupt_handler)
    args = opts()
    sync_preds()
