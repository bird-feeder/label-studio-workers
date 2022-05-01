#!/usr/bin/env python
# coding: utf-8

import argparse
import json
import os
import shutil
from glob import glob
from pathlib import Path

import requests
from dotenv import load_dotenv
from loguru import logger
from requests.structures import CaseInsensitiveDict
from tqdm import tqdm
from tqdm.contrib import tzip


class MigrateToS3:

    def __init__(self,
                 minio_folder: str,
                 template_project_id: int,
                 old_project_ids: list,
                 images_per_folder: int = 1000):
        self.minio_folder = minio_folder
        self.template_project_id = template_project_id
        self.old_project_ids = old_project_ids
        self.images_per_folder = images_per_folder

    @staticmethod
    def make_headers() -> CaseInsensitiveDict:
        headers = CaseInsensitiveDict()
        headers['Content-type'] = 'application/json'
        headers['Authorization'] = f'Token {os.environ["TOKEN"]}'
        return headers

    def api_request(self, url, method='get', data=None) -> dict:
        headers = self.make_headers()
        request = {'url': url, 'method': method, 'data': data}
        logger.debug(f'Request: {request}')
        if method == 'get':
            resp = requests.get(url, headers=headers)
        elif method == 'post':
            resp = requests.post(url, headers=headers, data=data)
        elif method == 'patch':
            resp = requests.patch(url, headers=headers, data=data)
        response = resp.json()
        logger.debug(f'Response: {response}')
        return response

    def download_existing_project_tasks(self) -> list:
        all_existing_tasks_data = []
        for project_id in tqdm(self.old_project_ids):
            data = api_request(
                f'{os.environ["LS_HOST"]}/api/projects/{project_id}/export'
                '?exportType=JSON&download_all_tasks=true')
            all_existing_tasks_data.append(data)
        return all_existing_tasks_data

    def copy_data_to_minio(self) -> int:
        """Create folders with `n` images per folder from the images inside
        `minio_folder`"""
        files = sorted(glob(f'{self.minio_folder}/*'))
        i = len(files) / self.images_per_folder
        if i != int(i):
            i = int(i) + 1
        chunks = [
            files[i:i + self.images_per_folder]
            for i in range(0, len(files), self.images_per_folder)
        ]
        for chunk in tqdm(chunks, desc='Chunks'):
            chunk_folder = f'{self.minio_folder}/project-{str(i).zfill(4)}'
            Path(chunk_folder).mkdir()
            for file in tqdm(chunk, desc='Files'):
                shutil.move(file, chunk_folder)
            i += 1
        return list(range(1, len(chunks)))

    def create_new_projects(self, list_of_projects_to_create: list) -> tuple:
        template = self.api_request(
            f'{os.environ["LS_HOST"]}/api/projects/{template_project_id}/')
        project_ids = []

        for n in list_of_projects_to_create:
            data = copy.deepcopy(template)
            data.pop('id')
            data.pop('created_by')
            color = random.choice(sns.color_palette('husl', 50).as_hex())

            data.update({
                'title': f'project-{str(n).zfill(4)}',
                'color': color
            })
            request = {
                'url': f'{os.environ["LS_HOST"]}/api/projects/',
                'method': 'post',
                'data': json.dumps(data)
            }
            response = self.api_request(**request)
            project_ids.append(response['id'])
        return project_ids

    def add_and_sync_data_storage(self, list_of_projects_to_create,
                                  project_ids) -> list:
        for project_name_num, project_id in tzip(list_of_projects_to_create,
                                                 project_ids):
            project_name = f'project-{str(project_name_num).zfill(4)}'
            storage_dict = {
                "type": "s3",
                "presign": True,
                "title": project_name,
                "bucket": "data",
                "prefix": project_name,
                "use_blob_urls": True,
                "aws_access_key_id": os.environ['MINIO_ACCESS_KEY'],
                "aws_secret_access_key": os.environ['MINIO_SECRET_KEY'],
                "region_name": 'us-east-1',
                "s3_endpoint": os.environ['MINIO_ENDPOINT'],
                "recursive_scan": True,
                "project": project_id
            }
            request = {
                'url': f'{os.environ["LS_HOST"]}/api/storages/s3',
                'method': 'post',
                'data': json.dumps(storage_dict)
            }
            response = self.api_request(**request)

            storage_id = response['id']
            request = {
                'url':
                f'{os.environ["LS_HOST"]}/api/storages/s3/{storage_id}/sync',
                'method': 'post',
                'data': json.dumps({'project': project_id})
            }
            sync_response = self.api_request(**request)
        return

    def post_existing_annotations_to_new_projects(self, project_ids,
                                                  base_data) -> None:
        base_data_dicts = {}
        for x in base_data:
            base_data_dicts.update({Path(x['data']['image']).name: x})

        for project_id in project_ids:
            request = {
                'url':
                f'{os.environ["LS_HOST"]}/api/projects/{project_id}/export?'
                'exportType=JSON&download_all_tasks=true',
                'method':
                'get'
            }
            cur_project_tasks = self.api_request(**request)

            for item in tqdm(cur_project_tasks):
                base_data_dict = base_data_dicts.get(
                    Path(item['data']['image']).name)
                if base_data_dict:
                    if base_data_dict['annotations']:
                        for anno in base_data_dict['annotations']:
                            anno.pop('updated_at')
                            anno.pop('created_at')
                            anno.update({'task': item['id']})
                            request = {
                                'url': f'{os.environ["LS_HOST"]}/api/'
                                f'tasks/{item["id"]}/annotations/',
                                'method': 'post',
                                'data': json.dumps(anno)
                            }
                            response = self.api_request(**request)
        return

    def run(self):
        # STEP 1
        list_of_projects_to_create = self.copy_data_to_minio()
        # STEP 2
        project_ids = create_new_projects(list_of_projects_to_create)
        # STEP 3
        self.add_and_sync_data_storage(list_of_projects_to_create, project_ids)
        # STEP 4
        base_data = self.download_existing_project_tasks()
        # STEP 5
        self.post_existing_annotations_to_new_projects(project_ids, base_data)
        return


if __name__ == '__main__':
    load_dotenv()
    logger.add('logs.log')
    logger.warning(
        'ALL IMAGES SHOULD BE INSIDE ONE FOLDER. USE THAT FOLDER AS '
        'AN INPUT TO `--minio-folder`')
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--minio-folder',
        help='Path to the folder where the minio bucket data are stored',
        type=str)
    parser.add_argument(
        '--template-project-id',
        help='An id to a project to be used as a template to all new projects',
        type=int)
    parser.add_argument('--old-project-ids',
                        help='Comma-separated list of ids of old projects '
                        '(i.e., projects to migrate from)',
                        type=str)
    parser.add_argument('--images-per-folder',
                        help='Number of images per folder',
                        type=int,
                        default=1000)
    args = parser.parse_args()

    old_project_ids = args.old_project_ids.split(',')

    migrate = MigrateToS3(minio_folder=args.minio_folder,
                          template_project_id=args.template_project_id,
                          old_project_ids=old_project_ids,
                          images_per_folder=args.images_per_folder)
    migrate.run()