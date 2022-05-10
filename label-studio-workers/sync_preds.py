#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import traceback

import ray
import requests
from dotenv import load_dotenv
from loguru import logger
from pymongo.errors import DuplicateKeyError
from tqdm import tqdm

from utils import catch_keyboard_interrupt, get_project_ids_str, mongodb_db, \
    get_tasks_from_mongodb


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
    except Exception:  # noqa # temp debug
        print('>>>>>>>>>>>>>>>>>>>> Unexpected exception')  # temp debug
        print(traceback.format_exc())  # temp debug
        print('<<<<<<<<<<<<<<<<<<<<')  # temp debug


def get_project_pred_ids(db, project_id):
    tasks = get_tasks_from_mongodb(db=db,
                                   project_id=project_id,
                                   json_min=False,
                                   dump=False)
    existing_ids = db[f'project_{project_id}_preds'].find().distinct('_id')

    all_pred_ids = []
    for task in tasks:
        for pred_id in task['predictions']:
            if pred_id not in existing_ids:
                all_pred_ids.append(pred_id)
    return all_pred_ids


def process_preds(db, project_id):
    prediction_ids = get_project_pred_ids(db, project_id)
    if not prediction_ids:
        logger.debug(f'All predictions in project {project_id} are up-to-date')
        return

    futures = []
    for pred_id in prediction_ids:
        futures.append(get_pred_details.remote(pred_id))

    for future in tqdm(futures, desc='Futures'):
        try:
            result = ray.get(future)
        except Exception:  # noqa
            ray.cancel(future)
            logger.error(
                '>>>>>>>>>>>>>>>>>>>> Unexpected exception')  # temp debug
            logger.error(traceback.format_exc())  # temp debug
            logger.error('<<<<<<<<<<<<<<<<<<<<')  # temp debug
            continue
        if isinstance(result, dict):
            result.update({'_id': result['id']})
            try:
                db[f'project_{project_id}_preds'].insert_one(result)
            except DuplicateKeyError:
                db[f'project_{project_id}_preds'].delete_one(
                    {'_id': result['_id']})
                db[f'project_{project_id}_preds'].insert_one(result)
        else:
            logger.error('Result is not instance of dict!')  # temp debug
            logger.error(f'Result: {result}')  # temp debug
    return


def sync_preds():
    catch_keyboard_interrupt()

    db = mongodb_db(os.environ['DB_CONNECTION_STRING'])

    if not args.project_ids:
        project_ids = get_project_ids_str().split(',')
    else:
        project_ids = args.project_ids.split(',')

    for project_id in tqdm(project_ids, desc='Projects'):
        process_preds(db, project_id)
    return


if __name__ == '__main__':
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--project-ids',
                        help='Comma-seperated project ids',
                        type=str)
    args = parser.parse_args()

    sync_preds()