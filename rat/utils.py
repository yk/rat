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
import subprocess

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


def async_system_call(cmd):
    p = subprocess.Popen(cmd.strip().split(' '), cwd=os.getcwd())
    return p


def echo(s):
    print(s)
    return s+'a'

def sleepecho(s):
    time.sleep(3)
    echo(s)

def get_all_files(directory):
    ls = []
    for e in os.listdir(directory):
        if e.startswith('.') or e.startswith('~') or e.startswith('_'):
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


def save_file_tree(grid, base_dir, filenames, exclude_patterns=[]):
    fids = []
    for fn in filenames:
        if any(p in fn for p in exclude_patterns):
            continue
        abs_fn = os.path.join(base_dir, fn)
        logging.info("saving %s from %s", fn, abs_fn)
        with open(abs_fn, 'rb') as f:
            fid = grid.put(f, filename=fn)
            fid = str(fid)
        fids.append(fid)
    return fids

def load_file_tree(grid, base_dir, file_ids, exclude_patterns=[]):
    files = []
    for fid in file_ids:
        gfile = grid.get(fid if isinstance(fid, ObjectId) else ObjectId(fid))
        fn = gfile.name or str(gfile._id)
        if any(p in fn for p in exclude_patterns):
            continue
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


def rsync_remote_folder(host, remote_path, local_path):
    cmd = 'rsync -avz "{}:{}/*" {}/'.format(host, remote_path, local_path)
    system_call(cmd)


def wait_for_running(db, experiment_id, config_id, interval=10):
    while True:
        exp = db.experiments.find_one({'_id': experiment_id})
        try:
            c = next(c for c in exp['configs'] if c['_id'] == config_id and c['status'] == Status.running)
            logging.info('{} is running'.format(config_id))
            break
        except StopIteration:
            time.sleep(interval)
    return c


def tail_remote_file(host, remote_path, local_path, interval=5):
    # cmd = 'unbuffer ssh {} "tail -qf -c+0 {} | base64" | gbase64 --decode -i > "{}"'.format(host, remote_path, local_path)
    # system_call(cmd, False)
    cmd = 'rsync -a "{}:{}" "{}"'.format(host, remote_path, local_path)
    while True:
        try:
            system_call(cmd)
            time.sleep(interval)
        except Exception as e:
            print(e)
            break


def dict_to_flags(d : dict):
    return " ".join(['--{0}={2}{1}{2}'.format(k, v, '"' if isinstance(v, str) else '') for k, v in d.items() if not k == 'main_file'])


def dict_to_list(d: dict):
    return " ".join(['{0}={2}{1}{2}'.format(k, v, '"' if isinstance(v, str) else '') for k, v in d.items() if not k == 'main_file'])



def dict_to_with(d : dict):
    return " ".join(['with {}="{}"'.format(k, v) for k, v in d.items()])

