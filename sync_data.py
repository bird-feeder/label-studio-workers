#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import signal
import sys
import time
from pathlib import Path

import ray
import schedule
from dotenv import load_dotenv
from loguru import logger

from create_rare_classes_view import CreateRareClassesView
from sync_images import sync_images
from sync_local_storage import sync_local_storage
from sync_tasks import sync_tasks


class MissingEnvironmentVariable(Exception):
    pass


def keyboard_interrupt_handler(sig, _):
    logger.warning(f'KeyboardInterrupt (ID: {sig}) has been caught...')
    logger.warning('Shutting down ray...')
    ray.shutdown()
    logger.warning('Terminating the session gracefully...')
    sys.exit(1)


def opts():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o',
                        '--once',
                        help='Run once then exit',
                        action='store_true')
    return parser.parse_args()


def main():
    start = time.time()

    logger.info('Running `create_rare_classes_view`...')
    project_ids = os.environ['PROJECTS_ID'].split(',')
    for project_id in project_ids:
        logger.debug(f'Current project id: {project_id}')
        create_rare_classes_view = CreateRareClassesView(project_id=project_id,
                                                        model_version='latest',
                                                        method='median')
        _ = create_rare_classes_view.create_view()

    logger.info('Running `sync_local_storage`...')
    sync_local_storage()

    logger.info('Running `sync_tasks`...')
    sync_tasks()

    if os.getenv('LOCAL_DB_CONNECTION_STRING'):
        logger.info('Running `sync_images`...')
        sync_images()

    logger.info(f'End. Took {round(time.time() - start, 2)}s')


if __name__ == '__main__':
    load_dotenv()
    logger.add(f'{Path(__file__).parent}/logs.log')
    signal.signal(signal.SIGINT, keyboard_interrupt_handler)
    args = opts()

    if args.once:
        main()
        sys.exit(0)

    schedule.every().day.at('06:00').do(main)
    schedule.every().day.at('10:00').do(main)

    while True:
        ray_is_running = False
        schedule.run_pending()
        time.sleep(1)
