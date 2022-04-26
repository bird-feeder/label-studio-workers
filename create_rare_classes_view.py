#!/usr/bin/env python
# coding: utf-8

import argparse
import json
import os

import numpy as np
import requests
from dotenv import load_dotenv
from loguru import logger

from mongodb_helper import mongodb_db


class CreateRareClassesView:

    def __init__(self, project_id, model_version, method='median'):
        self.project_id = project_id
        self.model_version = model_version
        self.method = method

    @staticmethod
    def make_headers():
        headers = requests.structures.CaseInsensitiveDict()  # noqa
        headers['Authorization'] = f'Token {os.environ["TOKEN"]}'
        headers['Content-type'] = 'application/json'
        return headers

    def create_view(self):
        db = mongodb_db(os.environ['DB_CONNECTION_STRING'])
        if self.model_version == 'latest':
            latest_model_ts = max(db.model.find().distinct('added_on'))
            d = db.model.find_one({'added_on': latest_model_ts})
        else:
            d = db.model.find_one({'_id': self.model_version})
        logger.debug(f'Model version: {d["_id"]}')

        labels_vals = list(d['labels'].values())
        if self.method == 'mean':
            count_m = np.mean(labels_vals)
        elif self.method == 'median':
            count_m = np.median(labels_vals)

        excluded_labels = os.getenv('EXCLUDE_LABELS')
        if excluded_labels:
            excluded_labels = excluded_labels.split(',')
        else:
            excluded_labels = []

        labels_with_few_annos = []
        for k, v in d['labels'].items():
            if count_m > v and k not in excluded_labels:
                labels_with_few_annos.append(k)

        headers = self.make_headers()

        view_template = {
            'data': {
                'type': 'list',
                'title': 'DONT_REMOVE_THIS_TAB',
                'target': 'tasks',
                'gridWidth': 4,
                'columnsWidth': {},
                'hiddenColumns': {
                    'explore': [
                        'tasks:annotations_results', 'tasks:annotations_ids',
                        'tasks:predictions_score',
                        'tasks:predictions_model_versions',
                        'tasks:predictions_results', 'tasks:file_upload',
                        'tasks:created_at', 'tasks:updated_at'
                    ],
                    'labeling': [
                        'tasks:id', 'tasks:completed_at',
                        'tasks:cancelled_annotations',
                        'tasks:total_predictions', 'tasks:annotators',
                        'tasks:annotations_results', 'tasks:annotations_ids',
                        'tasks:predictions_score',
                        'tasks:predictions_model_versions',
                        'tasks:predictions_results', 'tasks:file_upload',
                        'tasks:created_at', 'tasks:updated_at'
                    ]
                },
                'columnsDisplayType': {},
                'filters': {
                    'conjunction':
                    'or',
                    'items': [{
                        'filter': 'filter:tasks:predictions_results',
                        'operator': 'equal',
                        'type': 'String',
                        'value': 'placeholder_a'
                    }, {
                        'filter': 'filter:tasks:predictions_results',
                        'operator': 'equal',
                        'type': 'String',
                        'value': 'placeholder_b'
                    }]
                }
            }
        }

        filterd_labels = []
        for label in labels_with_few_annos:
            filterd_labels.append({
                'filter': 'filter:tasks:predictions_results',
                'operator': 'contains',
                'type': 'String',
                'value': label
            })

        view_template['data']['filters']['conjunction'] = 'or'
        view_template['data']['filters']['items'] = filterd_labels
        view_template['data']['title'] = 'rare_classes'

        view_template.update({'project': self.project_id})

        url = f'{os.environ["LS_HOST"]}/api/dm/views?project={self.project_id}'
        resp = requests.get(url, headers=headers)
        existing_rare_classes_tab = [
            x for x in resp.json() if x['data']['title'] == 'rare_classes'
        ]

        if existing_rare_classes_tab:
            if existing_rare_classes_tab[0]['data']['filters'][
                    'items'] == filterd_labels:
                logger.debug(
                    'An identical `rare_classes` view already exists for '
                    f'project {self.project_id}. Skipping...')
                return
            else:
                logger.debug(
                    'The list of rare classes has changed! Replacing...')
                existing_view_id = existing_rare_classes_tab[0]['id']
                url = f'{os.environ["LS_HOST"]}/api/dm/views/{existing_view_id}'
                resp = requests.delete(url, headers=headers)


        url = f'{os.environ["LS_HOST"]}/api/dm/views/'
        logger.debug(f'Request: {url} -d {view_template}')
        resp = requests.post(url,
                             headers=headers,
                             data=json.dumps(view_template))
        new_view = resp.json()
        logger.debug(f'Response: {new_view}')
        return new_view


if __name__ == '__main__':
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--project-id', help='Project ID', required=True)
    parser.add_argument('-v',
                        '--model-version',
                        help='Model version',
                        type=str,
                        required=True)
    parser.add_argument(
        '-m',
        '--method',
        type=str,
        help='The method used to calculate underrepresented classes',
        choices=['mean', 'median'],
        default='median')
    args = parser.parse_args()

    create_rare_classes_view = CreateRareClassesView(
        project_id=args.project_id,
        model_version=args.model_version,
        method=args.method)
    _ = create_rare_classes_view.create_view()
