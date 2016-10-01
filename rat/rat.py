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
from bson import ObjectId

rat_config = rcfile('rat')
db, grid = utils.get_mongo(rat_config)
rq = utils.get_redis(rat_config)

logging.root.setLevel(logging.INFO)


def run_config(experiment, config_id, configspec):
    cwd = os.getcwd()
    ls = utils.get_all_files(cwd)
    fids = utils.save_file_tree(grid, cwd, ls, experiment['_id'], config_id)
    config = dict(spec=configspec)
    config['_id'] = config_id
    config['files'] = list(zip(ls, fids))
    config['status'] = Status.enqueued
    db.experiments.update({'_id': experiment['_id']}, {'$push': {'configs': config}})

    rq.enqueue(worker.run_config, rat_config, experiment, config)

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
    e = db.experiments.find_one({'_id': {'$regex': '^{}'.format(search_string)}})
    if raise_if_none and not e:
        raise Exception('Experiment {} not found'.format(search_string))
    return e


def find_experiment_id(search_string, raise_if_none=True):
    e = db.experiments.find_one({'_id': {'$regex': '^{}'.format(search_string)}}, {'_id': 1})
    if raise_if_none and not e:
        raise Exception('Experiment {} not found'.format(search_string))
    return e


def delete(experiment):
    orphans = get_file_ids_for_experiment(experiment)
    delete_grid_files(orphans)
    db.experiments.remove(experiment['_id'])


def cmdline_delete(args):
    exp = find_experiment(args.search_string)
    delete(exp)


def status():
    exps = db.experiments.find({})
    return exps


def cmdline_status(args):
    def get_table():
        exps = status()
        table_data = [['Id', 'Name', 'Status', 'Start Time']]
        for e in exps:
            table_data.append([e['_id'][:6], e['name'], Status(e['status']).name, time.ctime(e['start_time'])])
        return AsciiTable(table_data).table

    if args.follow:
        utils.display_continuous(get_table, 1)
    else:
        print(get_table())


def get_files_for_experiment(experiment):
    return list(itertools.chain.from_iterable(map(lambda c: map(lambda f: (f[0], ObjectId(f[1])), itertools.chain(c.get('files', []), c.get('resultfiles', []))), experiment['configs'])))


def get_file_ids_for_experiment(experiment):
    fs = get_files_for_experiment(experiment)
    return [f[1] for f in fs]


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


def export(experiment, path):
    files = get_file_ids_for_experiment(experiment)
    utils.load_file_tree(grid, path, files)


def cmdline_export(args):
    exp = find_experiment(args.search_string)
    if args.temp:
        with tempfile.TemporaryDirectory() as path:
            export(exp, path)
            utils.system_call('open ' + path)
            print('press any key to exit')
            sys.stdin.read(1)
    else:
        path = args.path
        export(exp, path)


def main():
    try:
        parser = argparse.ArgumentParser(description="Experiment running tool")

        subparsers = parser.add_subparsers(dest="command", help="command")

        no_args_dict = {
            'clean': ('clean up saved experiments', cmdline_clean),
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
