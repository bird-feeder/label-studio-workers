#!/usr/bin/env python
# coding: utf-8

import os
import sys
from datetime import datetime
from pathlib import Path

import minio
import ray
from loguru import logger


def add_logger(current_file):
    Path('logs').mkdir(exist_ok=True)
    ts = datetime.now().strftime('%m-%d-%Y_%H.%M.%S')
    logs_file = f'logs/{Path(current_file).stem}_{ts}.log'
    logger.add(logs_file)
    return logs_file


def upload_logs(logs_file):
    client = minio.Minio(os.environ['MINIO_ENDPOINT'],
                         access_key=os.environ['MINIO_ACCESS_KEY'],
                         secret_key=os.environ['MINIO_SECRET_KEY'],
                         region=os.environ['MINIO_REGION'])
    file = Path(logs_file)
    try:
        logger.debug('Uploading logs...')
        resp = client.fput_object('logs', file.name, file)
        logger.debug(f'Uploaded log file: `{resp.object_name}`')
    except minio.error.S3Error as e:
        logger.error('Could not upload logs file!')
        logger.error(e)
    return


def keyboard_interrupt_handler(sig, _):
    logger.warning(f'KeyboardInterrupt (ID: {sig}) has been caught...')
    ray.shutdown()
    logger.warning('Terminating the session gracefully...')
    sys.exit(1)


def catch_keyboard_interrupt():
    return signal.signal(signal.SIGINT, keyboard_interrupt_handler)
