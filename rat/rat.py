#!/usr/bin/env python3

from rcfile import rcfile
import tempfile
import curses
import itertools
import logging
import argparse
import sys
import os
import uuid
import time
from rat import worker
from rat import utils
from rat.utils import Status
from terminaltables import AsciiTable
from multiprocessing import Process
from threading import Thread
from bson import ObjectId

rat_config = rcfile('rat')
db, grid = utils.get_mongo(rat_config)
rqueue = utils.get_redis(rat_config)

# logging.root.setLevel(logging.INFO)


def run_config(experiment, config_id, configspec):
    cwd = os.getcwd()
    ls = utils.get_all_files(cwd)
    fids = utils.save_file_tree(grid, cwd, ls)
    config = dict(spec=configspec)
    config['_id'] = config_id
    config['files'] = fids
    config['status'] = Status.enqueued
    db.experiments.update({'_id': experiment['_id']}, {'$push': {'configs': config}})

    rqueue.enqueue(worker.run_config, rat_config, experiment, config)

def run_experiment(configs, name=None):
    exp_id = str(uuid.uuid4())
    name = name or os.getcwd().split('/')[-1]
    experiment = {
            '_id': exp_id,
            'name': name,
            'configs': [],
            'start_time': time.time(),
            'status': Status.enqueued
            }
    db.experiments.insert_one(experiment)
    for cid, config in enumerate(configs):
        run_config(experiment, str(cid), config)


def find_experiment(search_string, raise_if_none=True):
    e = db.experiments.find({'_id': {'$regex': '^{}'.format(search_string)}})
    s = e.count()
    if s > 1:
        raise Exception('Ambiguous search string: {}'.format(search_string))
    if s < 1:
        if raise_if_none:
            raise Exception('Experiment {} not found'.format(search_string))
        return None
    return next(e)


def find_latest_experiment():
    return db.experiments.find_one({}, sort=[('start_time', -1)])


def find_experiment_id(search_string, raise_if_none=True):
    e = db.experiments.find_one({'_id': {'$regex': '^{}'.format(search_string)}}, {'_id': 1})
    if raise_if_none and not e:
        raise Exception('Experiment {} not found'.format(search_string))
    return e

def delete_all():
    nd = 0
    for exp in db.experiments.find({}):
        nd += 1
        delete(exp)
    return nd


def delete(experiment):
    orphans = get_file_ids_for_experiment(experiment)
    delete_grid_files(orphans)
    db.experiments.remove(experiment['_id'])


def cmdline_delete(args):
    exp = find_experiment(args.search_string)
    delete(exp)


def cmdline_delete_all(args):
    confirm()
    nd = delete_all()
    print('deleted {} experiments'.format(nd))
    clean()


def status():
    exps = db.experiments.find({}, limit=10, sort=[('start_time', -1)])
    return reversed(list(exps))


def cmdline_status(args):
    def get_table():
        exps = status()
        table_data = [['Id', 'Name', 'Status', 'Start Time', 'End Time']]
        for e in exps:
            end_time = time.ctime(e['end_time']) if 'end_time' in e else '-'
            table_data.append([e['_id'][:6], e['name'], Status(e['status']).name, time.ctime(e['start_time']), end_time])
        return AsciiTable(table_data).table

    if args.follow:
        utils.display_continuous(get_table, 1)
    else:
        print(get_table())


def get_file_ids_for_experiment(experiment):
    return list(itertools.chain.from_iterable(map(lambda c: map(lambda f: ObjectId(f), itertools.chain(c.get('files', []), c.get('resultfiles', []))), experiment['configs'])))


def get_file_ids_for_config(config):
    return list(map(lambda f: ObjectId(f), itertools.chain(config.get('files', []), config.get('resultfiles', []))))


def delete_grid_files(file_ids):
    no = 0
    for n, o in enumerate(file_ids):
        no = n
        grid.delete(o)
    return no



def clean():
    fileids = list(itertools.chain.from_iterable(map(lambda e: get_file_ids_for_experiment(e), db.experiments.find({}, {'configs.files': 1, 'configs.resultfiles': 1}))))
    orphans = grid.find({'_id': {'$nin': fileids}})
    return delete_grid_files([o._id for o in orphans])

def cmdline_clean(args):
    num_del = clean()
    print('{} orphans deleted'.format(num_del))


def export_experiment(experiment, path):
    for c in experiment['configs']:
        export_config(c, os.path.join(path, c['_id']))


def export_config(config, path, exclude_patterns=[]):
    files = get_file_ids_for_config(config)
    return utils.load_file_tree(grid, path, files, exclude_patterns=exclude_patterns)



def cmdline_export(args):
    exp = find_experiment(args.search_string)
    if args.temp:
        with tempfile.TemporaryDirectory() as path:
            export_experiment(exp, path)
            utils.system_call('open ' + path)
            print('>', end=' ')
            sys.stdin.read(1)
    else:
        path = args.path
        export_experiment(exp, path)


def wait_and_tail_logs(experiment, config, path):
    config = utils.wait_for_running(db, experiment['_id'], config['_id'])
    cpath = os.path.join(path, config['_id'])
    host, rpath = config['host'], config['path']
    utils.rsync_remote_folder(host, rpath, cpath)
    clogsdir = os.path.join(cpath, 'logs')
    tfefn = next(f for f in os.listdir(clogsdir) if 'tfevents' in f)
    logging.info('tailing %s from config %s', tfefn, config['_id'])
    utils.tail_remote_file(host, os.path.join(rpath, 'logs') + '/*tvevents*', os.path.join(cpath, 'logs') + '/*tvevents*')


def tensorboard(experiment, port):
    with tempfile.TemporaryDirectory() as path:
        done_configs = []
        not_done_configs = []
        for c in experiment['configs']:
            export_config(c, os.path.join(path, c['_id']), ['model', 'latest'])
            s = Status(c['status'])
            if s == Status.done:
                done_configs.append(c)
            elif s < Status.done:
                not_done_configs.append(c)
        processes = []
        for c in not_done_configs:
            # p = Process(target=wait_and_tail_logs, args=(experiment, c, path))
            p = Thread(target=wait_and_tail_logs, args=(experiment, c, path))
            p.start()
            processes.append(p)
        import tensorflow as tf
        from tensorflow.tensorboard.tensorboard import main as tbmain
        flags = tf.app.flags.FLAGS
        flags.port = port
        done_configs_logdirs = [utils.dict_to_list(c['spec']) + ':' + os.path.join(path, c['_id'], 'logs') for c in (done_configs + not_done_configs)]
        flags.logdir = ",".join(done_configs_logdirs)
        flags.reload_interval = 10
        try:
            tbmain()
        finally:
            pass
            # for p in processes:
                # logging.info('terminating %s', str(p))
                # p.terminate()



def cmdline_tb(args):
    port = args.port
    if args.search_string == 'latest':
        exp = find_latest_experiment()
    else:
        exp = find_experiment(args.search_string)
    tensorboard(exp, port)


def confirm(prompt='Really?'):
    a = input(prompt + "[yN]")
    if a == 'y' or a == 'Y':
        return
    exit(0)


def main():
    try:
        parser = argparse.ArgumentParser(description="Experiment running tool")

        subparsers = parser.add_subparsers(dest="command", help="command")

        no_args_dict = {
            'clean': ('clean up saved experiments', cmdline_clean),
            'deleteall': ('delete all experiments', cmdline_delete_all),
        }

        for k, v in no_args_dict.items():
            sp = subparsers.add_parser(k, help=v[0])
            sp.set_defaults(func=v[1])

        parser_status = subparsers.add_parser("status", help='display status of running experiments')
        parser_status.add_argument('-f', '--follow', action='store_true', help='continuously output status')
        parser_status.set_defaults(func=cmdline_status)

        # parser_run = subparsers.add_parser("run", help="run an experiment")
        # parser_run.add_argument('experiment_file')
        # parser_run.add_argument('-s', '--shuffle', action='store_true', help="shuffle the created configurations before distributing")
        # parser_run.set_defaults(func=run)

        parser_delete = subparsers.add_parser("delete", help="delete an experiment")
        parser_delete.add_argument('search_string')
        parser_delete.set_defaults(func=cmdline_delete)

        parser_export = subparsers.add_parser("export", help="export an experiment")
        parser_export.add_argument('search_string')
        parser_export.add_argument('-p', '--path', help="the directory to export to", default='.')
        parser_export.add_argument('-t', '--temp', action='store_true', help='export to a temporary folder')
        parser_export.set_defaults(func=cmdline_export)

        parser_tb = subparsers.add_parser("tb", help="open tensorboard")
        parser_tb.add_argument('search_string', nargs='?', default='latest')
        parser_tb.add_argument('-p', '--port', default=6006, type=int)
        parser_tb.set_defaults(func=cmdline_tb)

        args = parser.parse_args()

        command = args.command
        if hasattr(args, 'func'):
            args.func(args)
        else:
            parser.parse_args(['-h'])
            exit(1)
        exit(0)
    finally:
        utils.close_mongo()  # ugly

if __name__ == '__main__':
    main()
