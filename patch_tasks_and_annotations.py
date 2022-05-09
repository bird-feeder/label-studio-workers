#!/usr/bin/env python
# coding: utf-8

import json
import os
import sys

import ray
from dotenv import load_dotenv
from tqdm import tqdm

from utils import api_request, catch_keyboard_interrupt, get_project_ids_str


@ray.remote(num_cpus=30)  # noqa
def patch_task_annotations_predictions(task):
    for _anno in task['annotations']:
        anno_id = _anno['id']
        for anno in _anno['result']:
            value = anno['value']
            label = value['rectanglelabels'][0]

            if data.get(label):
                new_label = f'{label} ({data[label]})'
                anno['value']['rectanglelabels'] = [new_label]
                url = f'{os.environ["LS_HOST"]}/api/annotations/{anno_id}/'
                api_request(url, method='patch', data=_anno)

    url = f'{os.environ["LS_HOST"]}/api/predictions?task={task["id"]}'
    preds = api_request(url, method='get')
    for _pred in preds:
        pred_id = _pred['id']
        for pred in _pred['result']:
            value = pred['value']
            label = value['rectanglelabels'][0]
            if data.get(label):
                new_label = f'{label} ({data[label]})'
                pred['value']['rectanglelabels'] = [new_label]
                url = f'{os.environ["LS_HOST"]}/api/predictions/{pred_id}'
                resp = api_request(url, method='patch', data=_pred)
    return


def main():
    catch_keyboard_interrupt()
    project_ids = get_project_ids_str().split(',')

    for project_id in tqdm(project_ids, desc='Projects'):
        tasks = api_request(
            f'{os.environ["LS_HOST"]}/api/projects/{project_id}/export'
            '?exportType=JSON&download_all_tasks=true')

        futures = []
        for task in tasks:
            futures.append(patch_task_annotations_predictions.remote(task))

        for future in tqdm(futures, desc='Futures'):
            ray.get(future)


if __name__ == '__main__':
    load_dotenv()
    if len(sys.argv) == 1:
        sys.exit('Pass a data file as an argument (JSON file: '
                 '{"old_label": "new_label", ...})')
    with open(sys.argv[1]) as j:
        data = json.load(j)
    main()

    ray.shutdown()
