import time
import os
from redis import Redis
from rq import Queue
from pymongo import MongoClient
from gridfs import GridFS
from bson import ObjectId
from enum import IntEnum
import logging
import curses

mongo_client = None


class Status(IntEnum):
    enqueued = 1
    running = 2
    done = 3
    aborted = 90
    error = 95


def system_call(cmd, raise_on_error=True):
    status = os.system(cmd)
    if status != 0 and raise_on_error:
        raise Exception("System call failed: {}".format(cmd))
    return status


def echo(s):
    print(s)
    return s+'a'

def sleepecho(s):
    time.sleep(3)
    echo(s)

def get_all_files(directory):
    ls = []
    for e in os.listdir(directory):
        if e.startswith('.') or e == 'ratfile.py':
            continue
        eabs = os.path.join(directory, e)
        if os.path.isfile(eabs):
            ls.append(e)
        elif os.path.isdir(eabs):
            ls.extend([os.path.join(e, f) for f in get_all_files(eabs)])
    return ls


def get_redis(config):
    redis_connection = Redis(host=config.get('redis_host', 'localhost'), port=config.get('redis_port', '6379'))
    redis_queue = Queue(name=config.get('redis_queue', 'default'), connection=redis_connection)
    return redis_queue


def get_mongo(config):
    global mongo_client
    mongo_client = mongo_client or MongoClient(host=config.get('mongo_host', 'localhost'), port=int(config.get('mongo_port', '27017')))
    mongo_db = mongo_client[config.get('mongo_db', 'rat')]
    mongo_grid = GridFS(mongo_db, collection="testfs")
    return mongo_db, mongo_grid


def close_mongo():
    mongo_client.close()


def save_file_tree(grid, base_dir, filenames, experiment_id, config_id):
    fids = []
    for fn in filenames:
        abs_fn = os.path.join(base_dir, fn)
        db_path = os.path.join(experiment_id, config_id, fn)
        logging.info("saving %s from %s as %s", fn, abs_fn, db_path)
        with open(abs_fn, 'rb') as f:
            fid = grid.put(f, filename=db_path)
            fid = str(fid)
        fids.append(fid)
    return fids

def load_file_tree(grid, base_dir, file_ids):
    files = []
    for fid in file_ids:
        gfile = grid.get(ObjectId(fid))
        fn = gfile.name or str(gfile._id)
        local_abs_path = os.path.abspath(os.path.join(base_dir, fn) )
        folder_path, _ = os.path.split(local_abs_path)
        os.makedirs(folder_path, exist_ok=True)
        logging.info("restoring %s as %s", fn, local_abs_path)
        with open(local_abs_path, 'wb') as f:
            f.write(gfile.read())
        files.append((fid, fn, local_abs_path))
    return files

def display_continuous(str_func, interval=5):
    scr = curses.initscr()
    while(True):
        scr.clear()
        scr.addstr(str_func())
        scr.refresh()
        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            break
    curses.endwin()
