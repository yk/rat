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
from rq import Worker, push_connection, pop_connection
import hashlib
from collections import Counter

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

    rqueue.enqueue(worker.run_config, rat_config, experiment, config, timeout=24 * 60 * 60)

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


def find_latest_running_or_done_experiment():
    return db.experiments.find_one({'$or': [{'status': Status.running}, {'status': Status.done}]}, sort=[('start_time', -1)])


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


def kill_all(delete_after=False):
    nd = 0
    for exp in reversed(list(db.experiments.find({}))):
        nd += 1
        kill(exp, delete_after=delete_after)
        time.sleep(1)
    return nd


def delete(experiment):
    orphans = get_file_ids_for_experiment(experiment)
    delete_grid_files(orphans)
    db.experiments.remove(experiment['_id'])


def cmdline_delete(args):
    exp = find_experiment(args.search_string)
    delete(exp)


def kill_config(experiment, config):
    push_connection(rqueue.connection)
    for w in Worker.all():
        try:
            j = w.get_current_job()
        except:
            continue
        if j is None: continue
        if j.args[1]['_id'] == experiment['_id'] and j.args[2]['_id'] == config['_id']:
            cmd = "ssh -q {} 'kill $(pgrep -P {})'".format(config['host'], w.name.split('.')[-1])
            try:
                utils.system_call(cmd, raise_on_error=True)
            except:
                logging.warn('could not execute system call: {}'.format(cmd))

    pop_connection()


def kill(experiment, delete_after=False):
    for j in rqueue.jobs:
        exp_args = j.args[1]
        if exp_args['_id'] == experiment['_id']:
            j.delete()
    for c in experiment['configs']:
        if c['status'] == Status.running:
            kill_config(experiment, c)
    db.experiments.update({'_id': experiment['_id']}, {'$set': {'status': Status.killed}})
    if delete_after:
        delete(experiment)



def cmdline_kill(args):
    exps = [find_experiment(ss) for ss in args.search_string]
    for exp in exps:
        kill(exp, delete_after=args.delete)


def cmdline_delete_all(args):
    confirm()
    nd = delete_all()
    print('deleted {} experiments'.format(nd))
    clean()

def cmdline_kill_all(args):
    confirm()
    nd = kill_all(delete_after=args.delete)
    print('killed {} experiments'.format(nd))
    clean()


def status():
    exps = db.experiments.find({}, limit=10, sort=[('start_time', -1)])
    exps = list(exps)
    for e in exps:
        cstats = {
                    Status.enqueued: 0,
                    Status.running: 0,
                    Status.done: 0,
                }
        for c in e['configs']:
            s = Status(c['status'])
            if s in cstats:
                cstats[s] += 1

        e['cstats'] = cstats

    jobs = rqueue.jobs
    return reversed(list(exps)), jobs


TIMEFORMAT = '%d.%m. %H:%M:%S'


def cmdline_status(args):
    def get_table():
        exps, jobs = status()
        table_data = [['Id', 'Name', 'Status', 'Q', 'R', 'D', 'Start Time', 'End Time']]
        for e in exps:
            cstats = e['cstats']
            q, r, d = cstats[Status.enqueued], cstats[Status.running], cstats[Status.done]
            end_time = time.strftime(TIMEFORMAT, time.localtime(e['end_time'])) if 'end_time' in e else '-'
            table_data.append([e['_id'][:6], e['name'], Status(e['status']).name, q, r, d, time.strftime(TIMEFORMAT, time.localtime(e['start_time'])), end_time])
        return AsciiTable(table_data).table + '\n' + '{} jobs in queue'.format(len(jobs))

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


def export_experiment(experiment, path, message=None):
    for c in experiment['configs']:
        export_config(c, os.path.join(path, c['_id']))
    if message is not None and len(message) > 0:
        with open(os.path.join(path, 'msg.txt'), 'w') as f:
            f.write(message)


def export_config(config, path, exclude_patterns=[]):
    files = get_file_ids_for_config(config)
    return utils.load_file_tree(grid, path, files, exclude_patterns=exclude_patterns, raise_on_error=False)



def cmdline_export(args):
    exp = find_experiment(args.search_string)
    if args.temp:
        with tempfile.TemporaryDirectory() as path:
            export_experiment(exp, path, args.message)
            utils.system_call('open ' + path)
            print('>', end=' ')
            sys.stdin.read(1)
    else:
        path = args.path
        export_experiment(exp, path, args.message)


def wait_and_tail_logs(experiment, config, cpath):
    # config = utils.wait_for_running(db, experiment['_id'], config['_id'])
    if config['status'] < Status.running: return
    # cpath = os.path.join(path, config['_id'])
    host, rpath = config['host'], config['path']
    utils.rsync_remote_folder(host, rpath, cpath)
    clogsdir = os.path.join(cpath, 'logs')
    tfefn = next(f for f in os.listdir(clogsdir) if 'tfevents' in f)
    logging.info('tailing %s from config %s', tfefn, config['_id'])
    # utils.tail_remote_file(host, os.path.join(rpath, 'logs') + '/*tvevents*', os.path.join(cpath, 'logs') + '/*tvevents*')


def tensorboard(experiment, port):
    with tempfile.TemporaryDirectory() as path:
        done_configs = []
        not_done_configs = []
        spec_items = sorted(itertools.chain.from_iterable([c['spec'].items() for c in experiment['configs']]), key=lambda p:p[0])
        ctr = [(k, len(set(g))) for k, g in itertools.groupby(spec_items, lambda p:p[0])]
        common_attrs = [k for k, c in ctr if c == 1]
        common_values = []
        for k in common_attrs:
            v = None
            for c in experiment['configs']:
                if k in c['spec']:
                    v = c['spec'][k]
                    break
            common_values.append(v)

        print('Common Attributes:')
        print('\n'.join([k + ": " + str(v) for k, v in zip(common_attrs, common_values)]))

        for c in experiment['configs']:
            for cc in common_attrs:
                if cc in c['spec']:
                    del c['spec'][cc]
            # cpath = os.path.join(path, c['_id'])
            cpath = os.path.join(path, utils.dict_to_list(c['spec']))
            # export_config(c, cpath, ['model', 'latest'])
            export_config(c, cpath, [])
            s = Status(c['status'])
            if s == Status.done:
                done_configs.append(c)
            elif s < Status.done:
                not_done_configs.append((c, cpath))
        processes = []
        for c, cpath in not_done_configs:
            # p = Process(target=wait_and_tail_logs, args=(experiment, c, path))
            p = Thread(target=wait_and_tail_logs, args=(experiment, c, cpath))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

        with utils.working_directory(path):
            import tensorflow as tf
            from tensorflow.tensorboard.tensorboard import main as tbmain
            from tensorflow.tensorboard.plugins.projector.plugin import ProjectorPlugin
            flags = tf.app.flags.FLAGS
            flags.port = port
            # done_configs_logdirs = [utils.dict_to_list(c['spec']) + ':' + os.path.join(c['_id'], 'logs') for c in (done_configs + not_done_configs)]
            # flags.logdir = ",".join(done_configs_logdirs)
            flags.logdir = '.'
            flags.reload_interval = 10
            try:
                print('running tensorboard in {}'.format(path))

                def _new_get_metadata(self, tensor_name, config):
                    try:
                        cppath = config.model_checkpoint_path.rsplit('/', 1)[0]
                        for e in config.embeddings:
                            if e.tensor_name == tensor_name:
                                mfn = e.metadata_path.rsplit('/', 1)[-1]
                                return os.path.join(cppath, mfn)
                    except:
                        pass
                    return None

                ProjectorPlugin._get_metadata_file_for_tensor = _new_get_metadata
                tbmain()
            finally:
                pass
                # for p in processes:
                    # logging.info('terminating %s', str(p))
                    # p.terminate()


def cmdline_tb(args):
    port = args.port
    if args.search_string == 'latest':
        exp = find_latest_running_or_done_experiment()
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

        parser_kill = subparsers.add_parser("kill", help="kill an experiment")
        parser_kill.add_argument('search_string', nargs='+')
        parser_kill.add_argument('-d', '--delete', action='store_true', help="delete after kill")
        parser_kill.set_defaults(func=cmdline_kill)

        parser_kill_all = subparsers.add_parser("killall", help="kill all experiments")
        parser_kill_all.add_argument('-d', '--delete', action='store_true', help="delete after kill")
        parser_kill_all.set_defaults(func=cmdline_kill_all)

        parser_export = subparsers.add_parser("export", help="export an experiment")
        parser_export.add_argument('search_string')
        parser_export.add_argument('-p', '--path', help="the directory to export to", default='.')
        parser_export.add_argument('-m', '--message', help="message to write into the folder", default=None)
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
