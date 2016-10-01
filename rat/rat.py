#!/usr/bin/env python3

from rcfile import rcfile
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

rat_config = rcfile('rat')
db, grid = utils.get_mongo(rat_config)
rq = utils.get_redis(rat_config)


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


def cmdline_find(args):
    pass


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


def clean():
    fileids = list(itertools.chain.from_iterable(map(lambda e: list(itertools.chain.from_iterable(map(lambda c: map(lambda f: f[1], itertools.chain(c.get('files', []), c.get('resultfiles', []))), e['configs']))), db.experiments.find({}, {'configs.files': 1, 'configs.resultfiles': 1}))))
    orphans = grid.find({'_id': {'$nin': fileids}})
    no = 0
    for n, o in enumerate(orphans):
        no = n
        grid.delete(o._id)
    return no

def cmdline_clean(args):
    num_del = clean()
    print('{} items deleted'.format(num_del))


def main():
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

    parser_find = subparsers.add_parser("find", help="find experiments")
    parser_find.add_argument('search_string')
    parser_find.set_defaults(func=cmdline_find)

    args = parser.parse_args()

    command = args.command
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.parse_args(['-h'])
        exit(1)
    exit(0)

if __name__ == '__main__':
    main()
