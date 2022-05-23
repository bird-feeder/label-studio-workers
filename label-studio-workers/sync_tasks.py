#!/usr/bin/env python
# coding: utf-8

import argparse
import copy
import os
from typing import Union

import numpy as np
import ray
from dotenv import load_dotenv
from loguru import logger
from tqdm import tqdm

from sync_preds import process_preds
from utils import (api_request, catch_keyboard_interrupt,
                    get_all_projects_tasks, get_project_ids_str, mongodb_db)


@ray.remote
def insert_many_chunks(chunk: np.ndarray, col_name: str) -> None:
    db = mongodb_db(os.environ['DB_CONNECTION_STRING'])
    col = db[col_name]
    col.insert_many(chunk.tolist())


def sync_all() -> None:
    logger.debug('Running `sync_all()`...')

    tasks = get_all_projects_tasks()
    preds = get_all_projects_tasks(get_predictions_instead=True)

    for res, res_name in zip([tasks, preds], ['tasks', 'preds']):
        logger.debug(f'Syncing all {res_name} to one collection...')
        db = mongodb_db(os.environ['DB_CONNECTION_STRING'])
        col_name = f'all_projects_{res_name}'
        col = db[col_name]
        col.drop()
        chunks = np.array_split(res, 10)
        desc = f'{res_name.capitalize()} chunks'

        futures = [
            insert_many_chunks.remote(chunk, col_name) for chunk in chunks
        ]
        _ = [ray.get(future) for future in tqdm(futures, desc=desc)]

        logger.debug(f'Finished syncing all {res_name} to one collection...')

    logger.debug('Finished running `sync_all()`...')
    return


@ray.remote
def run(project_id: Union[int, str],
        json_min: bool = False,
        force_update: bool = False):
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
        logger.warning(f'(project: {project_id}) Empty project! Skipping...')
        return
    anno_len_ls = project_data['num_tasks_with_annotations']
    pred_len_ls = project_data['total_predictions_number']

    ls_lens = (tasks_len_ls, anno_len_ls, pred_len_ls)
    logger.debug(f'(project: {project_id}) Tasks: {tasks_len_ls}')
    logger.debug(f'(project: {project_id}) Annotations: {anno_len_ls}')
    logger.debug(f'(project: {project_id}) Predictions: {pred_len_ls}')

    db = mongodb_db(os.environ['DB_CONNECTION_STRING'])
    if json_min:
        col = db[f'project_{project_id}_min']
    else:
        col = db[f'project_{project_id}']

    tasks_len_mdb = len(list(col.find({})))
    anno_len_mdb = len(list(col.find({"annotations": {'$ne': []}})))
    pred_len_mdb = len(list(db[f'project_{project_id}_preds'].find({})))

    mdb_lens = (tasks_len_mdb, anno_len_mdb, pred_len_mdb)

    if force_update or ((not json_min and ls_lens != mdb_lens) or
                        (json_min and anno_len_ls != anno_len_mdb)):
        _msg = lambda x: f'Difference in {x} number'  # noqa
        logger.debug(
            f'(project: {project_id}) Project has changed. Updating...')
        logger.debug(f'(project: {project_id}) {_msg("tasks")}: '
                     f'{tasks_len_ls - tasks_len_mdb}')
        logger.debug(f'(project: {project_id}) {_msg("annotations")}: '
                     f'{anno_len_ls - anno_len_mdb}')
        logger.debug(f'(project: {project_id}) {_msg("predictions")}: '
                     f'{pred_len_ls - pred_len_mdb}')

        if json_min:
            data = api_request(
                f'{os.environ["LS_HOST"]}/api/projects/{project_id}/export'
                '?exportType=JSON_MIN&download_all_tasks=true')
        else:
            data = api_request(
                f'{os.environ["LS_HOST"]}/api/projects/{project_id}/export'
                '?exportType=JSON&download_all_tasks=true')
            tasks = copy.deepcopy(data)

        for task in data:
            if json_min:
                img = task['image']
            else:
                img = task['data']['image']
            task.update({'_id': task['id'], 'data': {'image': img}})

        col.drop()
        col.insert_many(data)

    else:
        logger.debug(f'(project: {project_id}) No changes were detected...')

    if not json_min and (force_update or pred_len_ls != pred_len_mdb):
        logger.debug(f'(project: {project_id}) Syncing predictions...')
        process_preds(db, project_id, tasks)  # noqa

    logger.info(f'(project: {project_id}) Finished (json_min: {json_min}).')


def opts() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--project-ids',
                        help='Comma-seperated project ids',
                        type=str)
    parser.add_argument('-f',
                        '--force',
                        help='Force update',
                        action='store_true')
    return parser.parse_args()


def sync_tasks(force_update: bool = False):
    catch_keyboard_interrupt()

    if not args.project_ids:
        project_ids = get_project_ids_str().split(',')
    else:
        project_ids = args.project_ids.split(',')

    futures = [
        run.remote(project_id, json_min=False, force_update=force_update)
        for project_id in project_ids
    ]
    _ = [ray.get(future) for future in tqdm(futures, desc='Projects')]

    futures_min = [
        run.remote(project_id, json_min=True, force_update=force_update)
        for project_id in project_ids
    ]
    _ = [ray.get(future) for future in tqdm(futures_min, desc='Projects min')]

    return


if __name__ == '__main__':
    load_dotenv()
    args = opts()
    if args.force:
        logger.info('Invoked force update!')
    sync_tasks(force_update=args.force)
    sync_all()
