import copy
import json
import os
import random
import shlex
import signal
import subprocess
import sys
import time
from datetime import date
from datetime import datetime
from glob import glob
from pathlib import Path

import requests
import schedule
from dotenv import load_dotenv
from loguru import logger


def keyboard_interrupt_handler(sig, _):
    print(f'KeyboardInterrupt (ID: {sig}) has been caught...')
    print('Terminating the session gracefully...')
    sys.exit(1)


def _run(cmd):
    p = subprocess.run(shlex.split(cmd),
                       shell=False,
                       check=True,
                       capture_output=True,
                       text=True)
    logger.info(f'Process exit code: {p.returncode}')
    logger.debug(f'stdout: {p.stdout}')
    logger.debug(f'stderr: {p.stderr}')
    return p.stdout


def make_headers():
    headers = requests.structures.CaseInsensitiveDict()  # noqa
    headers['Content-type'] = 'application/json'
    headers['Authorization'] = f'Token {os.environ["TOKEN"]}'
    return headers


def handle_project():
    url = f'{os.environ["LS_HOST"]}/api/projects'
    resp = requests.get(url, headers=headers)
    response = resp.json()
    logger.debug(response)

    picam_projects = []
    for p in response['results']:
        if 'picam' in p['title']:
            picam_projects.append((p['id'], p['title'], p['task_number']))

    cmd_size = f'rclone {os.environ["IS_SHARED"]} size ' \
    f'{os.environ["REMOTE_PATH"]}'
    num_of_new_files = int(
        _run(cmd_size).split('Total objects:')[1].split(' (')[1].split(')')[0])
    logger.debug(f'num_of_new_files: {num_of_new_files}')

    picam_projects = sorted(picam_projects)[::-1]

    last_picam_project = picam_projects[0]
    size_if_added = last_picam_project[-1] + num_of_new_files
    logger.debug(f'size_if_added: {size_if_added}')

    if size_if_added > 10000:
        logger.debug('Creating new project...')
        template = copy.deepcopy([
            x for x in response['results'] if x['id'] == picam_projects[0][0]
        ][0])
        template = {
            k: v
            for k, v in template.items() if k in ['title', 'label_config']
        }
        new_project_title = 'picam-' + str(int(template['title'][-3:]) +
                                           1).zfill(3)
        color = '%06x' % random.randint(0, 0xFFFFFF)
        template.update({'title': new_project_title, 'color': color})

        url = f'{os.environ["LS_HOST"]}/api/projects'
        resp = requests.post(url, headers=headers, data=json.dumps(template))
        _response = resp.json()
        logger.debug(_response)
        proj_id_to_use = _response['id']
    else:
        proj_id_to_use = picam_projects[0][0]

    return proj_id_to_use


def sync_picam(project_id):
    dt = datetime.today().strftime('%m-%d-%Y')
    NEW_FOLDER_NAME = f'downloaded_{dt}'

    if not Path(f'{path_to_picam}/{NEW_FOLDER_NAME}').exists():
        logger.error(f'{path_to_picam}/{NEW_FOLDER_NAME} does not exist!')
        raise FileNotFoundError

    url = f'{os.environ["LS_HOST"]}/api/storages/localfiles?' \
    f'project={project_id}'
    logger.debug(f'Request: {url}')
    resp = requests.get(url, headers=headers)
    response = resp.json()
    logger.debug(response)
    if isinstance(response, dict):
        if response.get('status_code') == 404:
            raise ConnectionError

    _PATH = f'{path_to_picam_on_container}/{NEW_FOLDER_NAME}'
    logger.debug(_PATH)
    EXISTS = False

    for x in response:
        if x['path'] == _PATH:
            logger.warning('Storage folder already exists!')
            logger.debug(f'Existing storage: {x}')
            EXISTS = True
            break

    if not EXISTS:
        data = "{" + f'"path":"{_PATH}","title":"{NEW_FOLDER_NAME}",' \
        f'"use_blob_urls":"true","project":{project_id}' + "}"
        resp = requests.post(url, headers=headers, data=data)
        response = resp.json()
        logger.debug(f'Request URL: {url}')
        logger.debug(f'Request data: {data}')
        if response.get('status_code') == 400:
            raise Exception('Something is wrong.')
        logger.info(f'Create new local storage response: {response}')
        storage_id = response['id']
    else:
        storage_id = x['id']

    logger.debug('Running sync...')
    url = f'{os.environ["LS_HOST"]}/api/storages/localfiles/{storage_id}/sync'
    resp = requests.post(url, headers=headers)
    logger.info(f'Sync response: {resp.text}')


def rclone_files_handler(project_id):
    ts = f'downloaded_{date.today().strftime("%m-%d-%Y")}'
    source_path = f'{path_to_picam}/{ts}'
    Path(source_path).mkdir(exist_ok=True)

    logger.debug('Copying images from google drive to local storage')
    cmd_copy = f'rclone copy {os.environ["IS_SHARED"]} ' \
    f'{os.environ["REMOTE_PATH"]} {source_path} -P --stats-one-line ' \
    '--ignore-existing --transfers 32'
    _run(cmd_copy)

    imgs = glob(f'{source_path}/*.jpg')
    logger.info(f'Copied {len(imgs)} image(s).')

    for _ in range(2):
        sync_picam(project_id)
        logger.debug('Running again just in case...')
        time.sleep(2)

    logger.debug('Creating -downloaded directory in remote')
    cmd_mkdir = f'rclone {os.environ["IS_SHARED"]} mkdir ' \
    f'{os.environ["REMOTE_PATH"]}-downloaded/{ts}'
    _run(cmd_mkdir)

    logger.debug('Moving images between remote folders')
    cmd_move = cmd_move = f'rclone {os.environ["IS_SHARED"]} move ' \
    f'{os.environ["REMOTE_PATH"]} {os.environ["REMOTE_PATH"]}' \
    f'-downloaded/{ts} -P --stats-one-line --transfers 32'
    _run(cmd_move)


def main():
    logger.debug('--------------------START--------------------')
    proj_id_to_use = handle_project()
    rclone_files_handler(proj_id_to_use)
    logger.debug('--------------------END--------------------')


if __name__ == '__main__':
    load_dotenv()
    logger.add('sync_local_storage.log')
    signal.signal(signal.SIGINT, keyboard_interrupt_handler)

    headers = make_headers()
    path_to_picam = os.environ['PATH_TO_PICAM']
    path_to_picam_on_container = os.environ['PATH_TO_PICAM_ON_CONTAINER']

    main()  # run once before schedule
    schedule.every().day.do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)
