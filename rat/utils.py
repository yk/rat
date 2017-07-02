#!/usr/bin/env python3


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
import contextlib

mongo_client = None


@contextlib.contextmanager
def working_directory(path):
    """A context manager which changes the working directory to the given
    path, and then changes it back to its previous value on exit.

    """
    prev_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


class Status(IntEnum):
    enqueued = 1
    running = 2
    done = 3
    killed = 90
    error = 95

    def short(self):
        d = {
                Status.enqueued: 'Q',
                Status.running: 'R',
                Status.done: 'D',
                Status.killed: 'K',
                Status.error: 'E',
            }
        l = d[self]
        return l


def exclude_include_patterns(configspec):
    epat, ipat = [], []
    if 'exclude' in configspec:
        epat = configspec['exclude'].split(',')
    if 'include' in configspec:
        ipat = configspec['include'].split(',')
    # epat.append('ext/')
    return epat, ipat


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
    return s + 'a'


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
    if mongo_client is None:
        mongo_client = MongoClient(host=config.get('mongo_host', 'localhost'), port=int(config.get('mongo_port', '27017')))
    mongo_db = mongo_client[config.get('mongo_db', 'rat')]
    mongo_user = config.get('mongo_user', '')
    if mongo_user:
        mongo_pwd = config.get('mongo_pwd', '')
        mongo_db.authenticate(mongo_user, mongo_pwd, source='admin')
    mongo_grid = GridFS(mongo_db, collection="testfs")
    return mongo_db, mongo_grid


def close_mongo():
    mongo_client.close()


def ensure_fid(fid):
    return fid if isinstance(fid, ObjectId) else ObjectId(fid)


def gfile_name(gfile):
    return gfile.name or str(gfile._id)


def duplicate_file(grid, fid):
    with grid.get(ensure_fid(fid)) as gfile:
        fn = gfile_name(gfile)
        new_fid = grid.put(gfile, filename=fn)
    return str(new_fid)


def save_file_tree(grid, base_dir, filenames, exclude_patterns=[], include_patterns=[]):
    fids = []
    for fn in filenames:
        if any(p in fn for p in exclude_patterns):
            if not any(p in fn for p in include_patterns):
                continue
        abs_fn = os.path.join(base_dir, fn)
        logging.info("saving %s from %s", fn, abs_fn)
        with open(abs_fn, 'rb') as f:
            fid = grid.put(f, filename=fn)
            fid = str(fid)
        fids.append(fid)
    return fids


def load_file_tree(grid, base_dir, file_ids, exclude_patterns=[], include_patterns=[], raise_on_error=True):
    files = []
    for fid in file_ids:
        try:
            gfile = grid.get(ensure_fid(fid))
        except:
            if raise_on_error:
                raise
            logging.warning("could not restore file %s", str(fid))
            continue
        fn = gfile.name or str(gfile._id)
        if any(p in fn for p in exclude_patterns):
            continue
        local_abs_path = os.path.abspath(os.path.join(base_dir, fn))
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


def rsync_remote_folder(host, remote_path, local_path, excludes=[]):
    cmd = 'rsync --progress -h -e "ssh -q" -qavz {} "{}:{}/*" "{}/"'.format(" ".join(['--exclude "{}"'.format(e) for e in excludes]), host, remote_path, local_path)
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
    cmd = 'rsync --progress -h -e "ssh -q" -qa "{}:{}" "{}"'.format(host, remote_path, local_path)
    while True:
        try:
            system_call(cmd)
            time.sleep(interval)
        except Exception as e:
            print(e)
            break


def dict_to_flags(d : dict):
    return " ".join(['--{0}={2}{1}{2}'.format(k, v, '' if isinstance(v, str) else '') for k, v in d.items() if not k == 'main_file'])


def dict_to_list(d: dict):
    return " ".join(['{0}={2}{1}{2}'.format(k, v, '' if isinstance(v, str) else '') for k, v in d.items() if not k == 'main_file'])


def dict_to_with(d : dict):
    return " ".join(['with {}="{}"'.format(k, v) for k, v in d.items()])


if __name__ == '__main__':
    s = Status.running
    print(s.short())
