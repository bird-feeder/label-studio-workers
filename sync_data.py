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

from sync_images import sync_images
from sync_local_storage import sync_local_storage
from sync_tasks import sync_tasks


class MissingEnvironmentVariable(Exception):
    pass


def keyboard_interrupt_handler(sig, _):
    logger.warning(f'KeyboardInterrupt (ID: {sig}) has been caught...')
    if ray_is_running:
        logger.warning('Shutting down ray...')
        ray.shutdown()
    logger.warning('Terminating the session gracefully...')
    sys.exit(1)


def checks():
    for k in [
            'PROJECTS_ID', 'DB_CONNECTION_STRING', 'DB_NAME', 'LS_HOST',
            'SRV_HOST', 'TOKEN', 'PATH_TO_SRC_DIR',
            'PATH_TO_SRC_DIR_ON_CONTAINER', 'REMOTE_PATH',
            'REMOTE_DOWNLOADED_PATH'
    ]:
        if not os.getenv(k):
            raise MissingEnvironmentVariable(
                f'`{k}` is required, but is not defined in `.env`!')

    if os.getenv('PATH_TO_SRC_DIR'):
        if not Path(os.getenv('PATH_TO_SRC_DIR')).exists():
            raise FileNotFoundError(
                f'{os.getenv("PATH_TO_SRC_DIR")} does not exist!')


def opts():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o',
                        '--once',
                        help='Run once then exit',
                        action='store_true')
    return parser.parse_args()


def main():
    global ray_is_running
    start = time.time()

    logger.info('Running `sync_local_storage`')
    sync_local_storage()

    logger.info('Running `sync_tasks`')
    sync_tasks()

    if os.getenv('LOCAL_DB_CONNECTION_STRING'):
        ray_is_running = True
        logger.info('Running `sync_images`')
        sync_images()

    logger.info(f'End. Took {round(start - time.time(), 2)}')


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
