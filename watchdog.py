#!/usr/bin/env python
# coding: utf-8

import argparse
import copy
import imghdr
import shutil
import signal
import sys
import time
from glob import glob
from pathlib import Path

from PIL import Image, UnidentifiedImageError
from loguru import logger


class WatchDog:

    def __init__(self, root_data_folder, images_per_folder=1000):
        self.root_data_folder = root_data_folder
        self.images_per_folder = images_per_folder

    @staticmethod
    def keyboard_interrupt_handler(sig: int, _) -> None:
        logger.warning(f'KeyboardInterrupt (id: {sig}) has been caught...')
        logger.info('Terminating the session gracefully...')
        sys.exit(1)

    @staticmethod
    def validate_image_file(file: str) -> str:
        try:
            if imghdr.what(file) and Image.open(file):
                return file
        except UnidentifiedImageError:
            time.sleep(1)
            if imghdr.what(file) and Image.open(file):
                return file
            else:
                logger.warning(f'`{file}` is corrupted!')
                shutil.move(
                    file,
                    f'{Path(self.root_data_folder).parent}/data_corrupted')

    def generate_next_folder_name(self) -> str:
        project_folders = sorted(glob(f'{self.root_data_folder}/project-*'))
        num = str(int(project_folders[-1].split('project-')[-1]) + 1).zfill(4)
        Path(f'{self.root_data_folder}/project-{num}').mkdir()
        return f'{self.root_data_folder}/project-{num}'

    def refresh_source(self) -> str:
        folders = glob(f'{self.root_data_folder}/*')
        project_folders = glob(f'{self.root_data_folder}/project-*')
        new_folders = list(set(folders).difference(project_folders))
        if new_folders:
            logger.debug(f'New folder(s) detected: {new_folders}')
        new_files = []
        for new_folder in new_folders:
            cur_files = [
                x for x in glob(f'{new_folder}/**/*', recursive=True)
                if not Path(x).is_dir()
            ]
            if cur_files:
                new_files.append(cur_files)
        new_files = sum(new_files, [])
        return folders, project_folders, new_folders, new_files

    def arrange_new_data_files(self) -> None:
        Path(f'{Path(self.root_data_folder).parent}/data_corrupted').mkdir(
            exist_ok=True)

        folders, project_folders, new_folders, new_files = self.refresh_source(
        )

        not_filled_folders = []
        project_folders = sorted(glob(f'{self.root_data_folder}/project-*'))

        for folder in project_folders:
            folder_size = len(glob(f'{folder}/*'))
            if self.images_per_folder > folder_size:
                not_filled_folders.append((folder, folder_size))
                logger.debug(f'Not filled: {folder}, size: {folder_size}')

        if not_filled_folders:
            for folder, folder_size in not_filled_folders:
                for file in new_files:
                    if self.validate_image_file(file):
                        shutil.move(file, folder)
                        if len(glob(f'{folder}/*')) == 1000:
                            break

        folders, project_folders, new_folders, new_files = self.refresh_source(
        )

        i = len(new_files) / self.images_per_folder
        if i != int(i):
            i = int(i) + 1
        chunks = [
            new_files[i:i + self.images_per_folder]
            for i in range(0, len(new_files), self.images_per_folder)
        ]

        for chunk in chunks:
            dst = self.generate_next_folder_name()
            for file in chunk:
                if self.validate_image_file(file):
                    shutil.move(file, dst)

        for empty_folder in new_folders:
            contains_any_file = [
                x for x in glob(f'{empty_folder}/**/*', recursive=True)
                if not Path(x).is_dir()
            ]
            if not contains_any_file:
                shutil.rmtree(empty_folder)

    def watch(self):
        signal.signal(signal.SIGINT, self.keyboard_interrupt_handler)
        logger.debug('Started watchdog...')
        global_state = glob(f'{self.root_data_folder}/**/*', recursive=True)
        while True:
            local_state = glob(f'{self.root_data_folder}/**/*', recursive=True)
            if global_state != local_state:
                logger.debug('Detected change!')
                global_state = copy.deepcopy(local_state)
                self.arrange_new_data_files()
            time.sleep(60)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root-data-folder',
                        help='Path to the folder where all the data is kept',
                        type=str,
                        required=True)
    parser.add_argument('--images-per-folder',
                        help='Number of images per folder',
                        type=int,
                        default=1000)
    args = parser.parse_args()

    watch_dog = WatchDog(root_data_folder=args.root_data_folder,
                         images_per_folder=args.images_per_folder)
    watch_dog.watch()
