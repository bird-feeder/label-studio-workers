#!/usr/bin/env python
# coding: utf-8

import json
import os
import sys

import pymongo
from dotenv import load_dotenv


def mongodb_db(connection_string):
    """Connects to the MongoDB database.

    Returns
    -------
    db: pymongo.database.Database
        The MongoDB database.
    """
    client = pymongo.MongoClient(connection_string)
    return client[os.environ['DB_NAME']]


def get_tasks_from_mongodb(db, project_id: str, dump=True, json_min=False):
    """Get tasks from MongoDB.

    Parameters
    ----------
    db: pymongo.database.Database
        The MongoDB database.
    project_id : str
        The ID of the project to get tasks from.
    dump : bool
        Whether to dump the data to a JSON file.
    json_min : bool
        The data will be exported as JSON_MIN when set to True.

    Returns
    -------
    tasks : list
        A list of tasks.
    """
    if json_min:
        col = db[f'project_{project_id}_min']
    else:
        col = db[f'project_{project_id}']
    tasks = list(col.find({}))

    if dump:
        with open('tasks.json', 'w') as j:
            json.dump(tasks, j, indent=4)
    return tasks


if __name__ == '__main__':
    load_dotenv()
    if len(sys.argv) == 1:
        raise SystemExit('Missing project ID!')
    _db = mongodb_db(os.environ['DB_CONNECTION_STRING'])
    get_tasks_from_mongodb(_db, sys.argv[1])
