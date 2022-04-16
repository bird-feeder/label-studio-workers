import signal
import sys
import time
from pathlib import Path

import ray
import schedule
from dotenv import load_dotenv
from loguru import logger

from sync_local_storage import sync_local_storage
from sync_tasks import sync_tasks
from sync_images import sync_images


def keyboard_interrupt_handler(sig, _):
    logger.warning(f'KeyboardInterrupt (ID: {sig}) has been caught...')
    if ray_is_running:
        logger.warning('Shutting down ray...')
        ray.shutdown()
    logger.warning('Terminating the session gracefully...')
    sys.exit(1)


def main():
    global ray_is_running
    logger.info('Running `sync_local_storage`')
    sync_local_storage()

    logger.info('Running `sync_tasks`')
    sync_tasks()

    ray_is_running = True
    logger.info('Running `sync_images`')
    sync_images()


if __name__ == '__main__':
    load_dotenv()
    logger.add(f'{Path(__file__).parent}/logs.log')
    signal.signal(signal.SIGINT, keyboard_interrupt_handler)
    if '--once' in sys.argv:
        main()
        sys.exit(0)

    schedule.every().day.do(main)

    while True:
        ray_is_running = False
        schedule.run_pending()
        time.sleep(1)
