#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import sys
import time

import requests
import schedule
from dotenv import load_dotenv
from loguru import logger

from mongodb_helper import mongodb_db
from sync_preds import process_preds


def to_srv(url):
    return url.replace(f'{os.environ["LS_HOST"]}/data/local-files/?d=',
                       f'{os.environ["SRV_HOST"]}/')


def api_request(url):
    headers = requests.structures.CaseInsensitiveDict()
    headers['Authorization'] = f'Token {os.environ["TOKEN"]}'
    resp = requests.get(url, headers=headers)
    return resp.json()


def run(project_id, json_min=False):
    """This function is used to update the database with the latest data from
    the server.
    It takes the project id as an argument and then makes an API request to
    the server to get the number of tasks and annotations in the project.
    It then connects to the database and gets the number of tasks and
    annotations in the database.
    If the number of tasks and annotations in the database is not equal to
    the number of tasks and annotations in the server, then it makes another
    API request to get the data of all the tasks in the project.
    It then updates the database with the latest data.
    """
    project_data = api_request(
        f'{os.environ["LS_HOST"]}/api/projects/{project_id}/')

    tasks_len_ls = project_data['task_number']
    if tasks_len_ls == 0:
        logger.warning(f'No tasks in project {project_id}! Skipping...')
        return
    anno_len_ls = project_data['num_tasks_with_annotations']
    pred_len_ls = project_data['total_predictions_number']

    ls_lens = (tasks_len_ls, anno_len_ls, pred_len_ls)
    logger.debug(f'Project {project_id}:')
    logger.debug(f'Tasks: {tasks_len_ls}')
    logger.debug(f'Annotations: {anno_len_ls}')
    logger.debug(f'Predictions: {pred_len_ls}')

    db = mongodb_db(os.environ['DB_CONNECTION_STRING'])
    if json_min:
        col = db[f'project_{project_id}_min']
    else:
        col = db[f'project_{project_id}']

    tasks_len_mdb = len(list(col.find({})))
    anno_len_mdb = len(list(col.find({"annotations": {'$ne': []}})))
    pred_len_mdb = len(list(db[f'project_{project_id}_preds'].find({})))

    mdb_lens = (tasks_len_mdb, anno_len_mdb, pred_len_mdb)

    if (not json_min
            and ls_lens != mdb_lens) or (json_min
                                         and anno_len_ls != anno_len_mdb):
        _msg = lambda x: f'Difference in {x} number'
        logger.debug(f'Project {project_id} has changed. Updating...')
        logger.debug(f'{_msg("tasks")}: {tasks_len_ls - tasks_len_mdb}')
        logger.debug(f'{_msg("annotations")}: {anno_len_ls - anno_len_mdb}')
        logger.debug(f'{_msg("predictions")}: {pred_len_ls - pred_len_mdb}')

        if json_min:
            data = api_request(
                f'{os.environ["LS_HOST"]}/api/projects/{project_id}/export'
                '?exportType=JSON_MIN&download_all_tasks=true')
        else:
            data = api_request(
                f'{os.environ["LS_HOST"]}/api/projects/{project_id}/export'
                '?exportType=JSON&download_all_tasks=true')

        for task in data:
            if json_min:
                img = task['image']
            else:
                img = task['data']['image']
            task.update({
                '_id': task['id'],
                'data': {
                    '_image': to_srv(img),
                    'image': img
                }
            })

        col.drop()
        col.insert_many(data)

    else:
        logger.debug(f'No changes were detected in project {project_id}...')

    if pred_len_ls != pred_len_mdb:
        logger.debug('Syncing predictions...')
        process_preds(db, project_id)


def opts():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--projects-id',
                        help='Comma-seperated projects ID',
                        type=str)
    parser.add_argument('-o',
                        '--once',
                        help='Run once and exit',
                        action='store_true')
    return parser.parse_args()


def sync_tasks(projects_id=None):
    if not projects_id:
        projects_id = os.environ['PROJECTS_ID'].split(',')
    else:
        projects_id = projects_id.split(',')

    for is_json_min in [False, True]:
        for project_id in projects_id:
            run(project_id, is_json_min)
            logger.info(
                f'Finished processing project {project_id} (is_json_min: '
                f'{is_json_min})'
            )


if __name__ == '__main__':
    load_dotenv()
    logger.add('logs.log')
    args = opts()
    if args.once:
        sync_tasks(args.projects_id)
        sys.exit(0)
    schedule.every(10).minutes.do(sync_tasks)

    while True:
        schedule.run_pending()
        time.sleep(1)
