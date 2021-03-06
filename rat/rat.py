#!/usr/bin/env python3

import pickle
# pickle.HIGHEST_PROTOCOL = 4
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
from rat import hyperopt
from rat.utils import Status, SearchStatus
from terminaltables import AsciiTable
from multiprocessing import Process
from threading import Thread
from bson import ObjectId
from rq import Worker, push_connection, pop_connection
import hashlib
from collections import Counter
from tqdm import tqdm
import pymongo
import numpy as np
import json
import sh

rat_config = rcfile('rat')
db, grid = utils.get_mongo(rat_config)
rqueue = utils.get_redis(rat_config)

# logging.root.setLevel(logging.INFO)


def get_free_config_id(experiment):
    return str(max([int(c['_id']) for c in experiment['configs']] + [-1]) + 1)


def run_config(experiment, config_id, configspec):
    changes = [0]
    while len(changes) > 0:
        changes = []
        for k, v in configspec.items():
            if isinstance(v, str):
                if '$(' in v:
                    vbeg, vmid = v.split('$(', 1)
                    vmid, vend = vmid.split(')', 1)
                    vmid = configspec[vmid]
                    changes.append((k, '{}{}{}'.format(vbeg, vmid, vend)))
        for k, v in changes:
            configspec[k] = v

    config = dict(spec=configspec)
    config['_id'] = config_id
    config['status'] = Status.enqueued
    db.experiments.update({'_id': experiment['_id']}, {'$push': {'configs': config}})

    rqueue.enqueue(worker.run_config, rat_config, experiment, config, job_timeout=30 * 24 * 60 * 60, description=json.dumps(configspec))
    # rqueue.enqueue(worker.run_config, rat_config, experiment, config, description=json.dumps(configspec))


def get_config(experiment, config_id):
    config = [c for c in experiment['configs'] if c['_id'] == config_id]
    assert len(config) == 1
    config = config[0]
    return config


def rerun_config(experiment, config):
    free_id = get_free_config_id(experiment)
    # new_fids = utils.duplicate_files(grid, config['files'])
    run_config(experiment, free_id, config['spec'])


def restart_config(experiment, config):
    kill_config(experiment, config)
    rerun_config(experiment, config)


def run_experiment(spec, main_file, name=None, file_ids=None, search_strategy=None, command='python3', step_after=True):
    if not isinstance(spec, list):
        spec = [spec]
    spec = [{k: v.tolist() if isinstance(v, np.ndarray) else v for k, v in s.items()} for s in spec]

    if search_strategy is None:
        search_strategy = {
                'create': 'raw',
                'score': 'constant'
                }
    exp_id = str(uuid.uuid4())
    name = name or os.getcwd().split('/')[-1]
    experiment = {
            '_id': exp_id,
            'name': name,
            'configs': [],
            'spec': spec,
            'search_status': SearchStatus.enqueued,
            'search_init': False,
            'start_time': time.time(),
            'status': Status.enqueued,
            'main_file': main_file,
            'search_strategy': search_strategy,
            'state': {},
            'history': [],
            'command': command,
            }

    cwd = os.getcwd()
    ls = utils.get_all_files(cwd)
    epat, ipat = utils.exclude_include_patterns(spec[0])
    epat.append('ext/')

    if file_ids is None:
        file_ids = utils.save_file_tree(grid, cwd, ls, exclude_patterns=epat, include_patterns=ipat)

    experiment['files'] = file_ids
    db.experiments.insert_one(experiment)
    hyperopt.init_hyperopt(experiment['_id'])
    if step_after:
        hyperopt.do_hyperopt_steps(experiment['_id'])
    return experiment


def merge(experiments, name='merged'):
    exp = {
            '_id': 'mrg' + str(uuid.uuid4()),
            'name': name,
            'start_time': min([e['start_time'] for e in experiments if 'start_time' in e]),
            'end_time': max([e['end_time'] for e in experiments if 'end_time' in e]),
            'status': min([e['status'] for e in experiments if 'status' in e]),
            }
    configs = list(itertools.chain.from_iterable([e['configs'] for e in experiments]))
    for i, c in enumerate(configs):
        c['_id'] = str(i)
    exp['configs'] = configs
    db.experiments.insert_one(exp)


def find_experiment(search_string, raise_if_none=True, allow_relative=False):
    collection_name = 'experiments'
    if allow_relative and search_string and search_string.startswith('-'):
        n = int(search_string[1:])
        e = db[collection_name].find({}, sort=[('start_time', -1)], limit=n)
        if e.count() < n:
            raise Exception('Not enough {}'.format(collection_name))
        return list(e)[n-1]

    e = db[collection_name].find({'_id': {'$regex': '^{}'.format(search_string)}})
    s = e.count()
    if s > 1:
        raise ValueError('Ambiguous search string: {}'.format(search_string))
    if s < 1:
        if raise_if_none:
            raise ValueError('Search string {} not found'.format(search_string))
        return None
    return next(e)


def find_latest_running_or_done_experiment():
    return db.experiments.find_one({'$or': [{'status': Status.running}, {'status': Status.done}]}, sort=[('start_time', -1)])


def find_experiment_id(search_string, raise_if_none=True):
    e = db.experiments.find_one({'_id': {'$regex': '^{}'.format(search_string)}}, {'_id': 1})
    if raise_if_none and not e:
        raise ValueError('Experiment {} not found'.format(search_string))
    return e

def delete_all():
    nd = 0
    for exp in db.experiments.find({}):
        nd += 1
        delete(exp)
    return nd


def kill_all(name=None, limit=0, delete_after=False, force=False):
    nd = 0
    query = {}
    if name:
        query['name'] = name
    to_delete = list(db.experiments.find(query).sort('start_time', pymongo.DESCENDING).limit(limit))
    print('deleting {} experiments'.format(len(to_delete)))
    if not force:
        confirm()
    for exp in to_delete:
        nd += 1
        kill(exp, delete_after=delete_after)
        time.sleep(1)
    return nd


def trim():
    for e in list(db.experiments.find({'status': Status.enqueued}).sort('start_time', pymongo.DESCENDING)):
        kill(e, delete_after=True)


def delete(experiment, keep_files=False):
    if not keep_files:
        orphans = get_file_ids_for_experiment(experiment)
        delete_grid_files(orphans)
    db.experiments.remove(experiment['_id'])


def delete_config(experiment, config, keep_files=False):
    if not keep_files:
        orphans = get_file_ids_for_config(config)
        delete_grid_files(orphans)
    db.experiments.update({'_id': experiment['_id']}, {'$pull': {'configs': {'_id': config['_id']}}})


def cmdline_delete(args):
    exp = find_experiment(args.search_string)
    delete(exp, args.keep_files)


def cmdline_rerun(args):
    exp = find_experiment(args.search_string)
    config = get_config(exp, args.config_id)
    rerun_config(exp, config)


def cmdline_abort(args):
    exp = find_experiment(args.search_string)
    config = get_config(exp, args.config_id)
    if args.rerun:
        rerun_config(exp, config)
    else:
        kill_config(exp, config)


def kill_config(experiment, config):
    push_connection(rqueue.connection)
    for w in Worker.all():
        try:
            j = w.get_current_job()
        except:
            continue
        if j is None: continue
        if j.args[1]['_id'] == experiment['_id'] and j.args[2]['_id'] == config['_id']:
            cmd = "ssh -q {} 'kill $(pgrep -P {})'".format(config['host'], w.pid)
            try:
                utils.system_call(cmd, raise_on_error=True)
            except:
                logging.warn('could not execute system call: {}'.format(cmd))

    pop_connection()


def kill(experiment, delete_after=False, keep_files_on_delete=False):
    db.experiments.update({'_id': experiment['_id']}, {'$set': {'status': Status.killing, 'search_status': SearchStatus.done}})
    for j in rqueue.jobs:
        exp_args = j.args[1]
        if exp_args['_id'] == experiment['_id']:
            j.delete()
    for c in experiment['configs']:
        if c['status'] == Status.running:
            kill_config(experiment, c)
    db.experiments.update({'_id': experiment['_id']}, {'$set': {'status': Status.killed}})
    if delete_after:
        delete(experiment, keep_files_on_delete)


def cmdline_kill(args):
    exps = [find_experiment(ss) for ss in args.search_string]
    for exp in exps:
        kill(exp, delete_after=args.delete, keep_files_on_delete=args.keep_files)

def cmdline_merge(args):
    exps = [find_experiment(ss) for ss in args.search_string]
    merge(exps)


def get_hopt_experiment_ids(search_strings):
    if len(search_strings) == 0:
        exp_ids = [e['_id'] for e in db.experiments.find({'search_status': SearchStatus.running}, {'_id': 1})]
    else:
        exp_ids = [find_experiment(search_string, allow_relative=True)['_id'] for search_string in search_strings]
    return exp_ids


def cmdline_hopt_step(args):
    exp_ids = get_hopt_experiment_ids(args.search_strings)
    for eid in exp_ids:
        if args.single:
            hyperopt.do_hyperopt_step(eid)
        else:
            hyperopt.do_hyperopt_steps(eid)

def cmdline_hopt_monitor(args):
    hyperopt_monitor(args.search_strings, args.pause)


def hyperopt_monitor(search_strings, pause=10):
    try:
        print('monitoring...')
        while True:
            try:
                exp_ids = get_hopt_experiment_ids(search_strings)
                for eid in exp_ids:
                    hyperopt.do_hyperopt_steps(eid)
            except ValueError as e:
                logging.warn(e)
            time.sleep(pause)
    except KeyboardInterrupt:
        print('aborting monitor')

def cmdline_delete_all(args):
    confirm()
    nd = delete_all()
    print('deleted {} experiments'.format(nd))
    clean()

def cmdline_kill_all(args):
    nd = kill_all(name=args.name, limit=args.limit, delete_after=args.delete, force=args.force)
    print('killed {} experiments'.format(nd))
    clean(limit=args.batch)

def cmdline_trim(args):
    trim()

def status(limit=10):
    exps = db.experiments.find({}, limit=limit, sort=[('start_time', -1)])
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


def cmdline_info(args):
    def get_table(state):
        if args.search_string == 'latest':
            exp = find_latest_running_or_done_experiment()
        else:
            exp = find_experiment(args.search_string)
        common_attrs, common_values = get_common_attributes(exp)
        configs = exp['configs']
        table_data = [['Id', 'Status', 'Host', 'Parameters']]
        for c in configs:
            remove_common_attributes(c, common_attrs)
            spec = ' '.join(['{}={}'.format(sk, sv) for sk, sv in c['spec'].items()])

            table_data.append([c['_id'], Status(c['status']).name, c.get('host', '-'), spec])
        cas = ' '.join(['{}={}'.format(sk, sv) for sk, sv in zip(common_attrs, common_values)])
        return AsciiTable(table_data).table + '\n' + cas

    if args.follow:
        utils.display_continuous(get_table, 1)
    else:
        print(get_table({}))



def cmdline_status(args):
    def get_table(state):
        first_time = 'done' not in state
        if first_time:
            state['done'] = []
        exps, jobs = status(args.limit)
        table_data = [['Id', 'Name', 'Status', 'Q', 'R', 'D', 'Start Time', 'End Time']]
        for e in exps:
            cstats = e['cstats']
            q, r, d = cstats[Status.enqueued], cstats[Status.running], cstats[Status.done]
            end_time = time.strftime(TIMEFORMAT, time.localtime(e['end_time'])) if 'end_time' in e else '-'
            exp_id = e['_id'][:6]
            if not e.get('search_status', SearchStatus.done) >= SearchStatus.done:
                exp_id += '*'
            table_data.append([exp_id, e['name'], Status(e['status']).name, q, r, d, time.strftime(TIMEFORMAT, time.localtime(e['start_time'])), end_time])

            if Status(e['status']) == Status.done:
                if e['_id'] not in state['done']:
                    state['done'].append(e['_id'])
                    if not first_time:
                        utils.notify('Experiment done', '{} ({})'.format(e['name'], exp_id))

        return AsciiTable(table_data).table + '\n' + '{} jobs in queue'.format(len(jobs))

    if args.follow:
        utils.display_continuous(get_table, 1)
    else:
        print(get_table({}))


def get_file_ids_for_experiment(experiment, with_configs=True):
    # return list(itertools.chain.from_iterable(map(lambda c: map(lambda f: ObjectId(f), itertools.chain(c.get('files', []), c.get('resultfiles', []))), experiment['configs'])))
    if with_configs:
        return list(map(ObjectId, itertools.chain(itertools.chain.from_iterable(map(lambda c: c.get('resultfiles', []), experiment['configs'])), experiment.get('files', []))))
    return list(map(ObjectId, experiment['files']))


def get_file_ids_for_config(config):
    return list(map(lambda f: ObjectId(f), config.get('resultfiles', [])))


def delete_grid_files(file_ids):
    no = 0
    for n, o in enumerate(file_ids):
        no = n
        grid.delete(o)
    return no


def clean(limit=0):
    fileids = list(itertools.chain.from_iterable(map(get_file_ids_for_experiment, db.experiments.find({}, {'files': 1, 'configs.resultfiles': 1}))))
    fileids += list(itertools.chain.from_iterable(map(get_file_ids_for_config, db.hyperopt.find({}, {'files': 1}))))
    num_del = 0
    orphans = True
    while orphans:
        orphans = grid.find({'_id': {'$nin': fileids}}, limit=limit)
        if orphans:
            num_del += delete_grid_files([o._id for o in orphans])
    return num_del

def cmdline_clean(args):
    num_del = clean()
    print('{} orphans deleted'.format(num_del))


def export_experiment(experiment, path, configs=None, message=None, only_done=False):
    efids = get_file_ids_for_experiment(experiment, False)
    utils.load_file_tree(grid, path, efids, raise_on_error=True)

    cfgs = experiment['configs']
    if configs:
        cfgs = [c for c in cfgs if c['_id'] in configs]
    if only_done:
        cfgs = [c for c in cfgs if c['status'] == Status.done]
    for c in tqdm(cfgs):
        export_config(experiment, c, os.path.join(path, 'configs', c['_id']))
    if message is not None and len(message) > 0:
        with open(os.path.join(path, 'msg.txt'), 'w') as f:
            f.write(message)


def export_config(experiment, config, path, exclude_patterns=[], include_patterns=[], with_experiment_files=False):
    files = get_file_ids_for_config(config)
    if with_experiment_files:
        files += get_file_ids_for_experiment(experiment, False)
    res = utils.load_file_tree(grid, path, files, exclude_patterns=exclude_patterns, include_patterns=include_patterns, raise_on_error=False)
    logpath = os.path.join(path, 'logs')
    if os.path.exists(logpath) and os.path.isdir(logpath):
        for txt in ('cmdlog', 'stdout', 'stderr'):
            txt = txt + '.txt'
            fn = os.path.join(path, txt)
            if os.path.exists(fn):
                sh.cp(fn, logpath)
    return res


def cmdline_export(args):
    exp = find_experiment(args.search_string)
    configs = None
    if args.configs:
        configs = args.configs.split(',')
    if args.temp:
        with tempfile.TemporaryDirectory() as path:
            export_experiment(exp, path, configs=configs, message=args.message, only_done=args.done)
            utils.system_call('open ' + path)
            print('>', end=' ')
            sys.stdin.read(1)
    else:
        path = args.path
        export_experiment(exp, path, configs=configs, message=args.message, only_done=args.done)


def cmdline_stats(args):
    exp = find_experiment(args.search_string, allow_relative=True)
    hyperopt.do_hyperopt_stats(exp)


def wait_and_tail_logs(experiment, config, cpath, checkpoints=False):
    # config = utils.wait_for_running(db, experiment['_id'], config['_id'])
    if config['status'] < Status.running: return
    # cpath = os.path.join(path, config['_id'])
    host, rpath = config['host'], config['path']
    excludes = ['ext/*']
    if not checkpoints:
        excludes += ['*.ckpt*']
    utils.rsync_remote_folder(host, rpath, cpath, excludes=excludes)
    clogsdir = os.path.join(cpath, 'logs')
    # tfefn = next(f for f in os.listdir(clogsdir) if 'tfevents' in f)
    # logging.info('tailing %s from config %s', tfefn, config['_id'])
    # utils.tail_remote_file(host, os.path.join(rpath, 'logs') + '/*tvevents*', os.path.join(cpath, 'logs') + '/*tvevents*')


def sync_configs(experiment, configs_and_paths, checkpoints=False):
    processes = []
    for c, cpath in configs_and_paths:
        # p = Process(target=wait_and_tail_logs, args=(experiment, c, path))
        p = Thread(target=wait_and_tail_logs, args=(experiment, c, cpath, checkpoints))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()


def get_common_attributes(experiment):
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
    return common_attrs, common_values


def remove_common_attributes(config, common_attributes):
    for cc in common_attributes:
        if cc in config['spec']:
            del config['spec'][cc]


def tensorboard(experiment, port, checkpoints=False, info_only=False, done_only=False, running_only=False):
    with tempfile.TemporaryDirectory() as path:
        done_configs = []
        not_done_configs = []
        common_attrs, common_values = get_common_attributes(experiment)

        print('Common Attributes:')
        print('\n'.join([k + ": " + str(v) for k, v in zip(common_attrs, common_values)]))
        if info_only:
            return

        for c in experiment['configs']:
            remove_common_attributes(c, common_attrs)
            # cpath = os.path.join(path, c['_id'])
            cpath = os.path.join(path, utils.dict_to_list(c['spec']).replace('/', '__'), c['_id'])
            # export_config(c, cpath, ['model', 'latest'])
            epat, _ = utils.exclude_include_patterns(c['spec'])
            if not checkpoints:
                epat.append('.ckpt')
            epat.append('ext/')
            export_config(experiment, c, cpath, exclude_patterns=epat)
            s = Status(c['status'])
            if s == Status.done and not running_only:
                done_configs.append(c)
            elif s < Status.done:
                not_done_configs.append((c, cpath))
        processes = []
        if not done_only:
            sync_configs(experiment, not_done_configs, checkpoints=checkpoints)

        with utils.working_directory(path):
            import tensorflow as tf
            from tensorboard.main import run_main as tbmain
            from tensorboard.plugins.projector.projector_plugin import ProjectorPlugin
            flags = {}
            flags['port'] = port
            # done_configs_logdirs = [utils.dict_to_list(c['spec']) + ':' + os.path.join(c['_id'], 'logs') for c in (done_configs + not_done_configs)]
            # flags.logdir = ",".join(done_configs_logdirs)
            flags['logdir'] = '.'
            flags['reload_interval'] = 10
            sys.argv.extend(utils.dict_to_flags(flags).split())
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
        exp = find_experiment(args.search_string, allow_relative=True)
    del sys.argv[1:]
    tensorboard(exp, port, args.checkpoints, args.info_only, done_only=args.done, running_only=args.running)


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
        parser_status.add_argument('-l', '--limit', default=10, type=int, help='how many experiments to show')
        parser_status.set_defaults(func=cmdline_status)

        # parser_run = subparsers.add_parser("run", help="run an experiment")
        # parser_run.add_argument('experiment_file')
        # parser_run.add_argument('-s', '--shuffle', action='store_true', help="shuffle the created configurations before distributing")
        # parser_run.set_defaults(func=run)

        parser_info = subparsers.add_parser("info", help="info of an experiment")
        parser_info.add_argument('search_string', nargs='?', default='latest')
        parser_info.add_argument('-f', '--follow', action='store_true', help='continuously output status')
        parser_info.set_defaults(func=cmdline_info)

        parser_delete = subparsers.add_parser("delete", help="delete an experiment")
        parser_delete.add_argument('search_string')
        parser_delete.add_argument('-F', '--keep_files', action='store_true', help="keep files")
        parser_delete.set_defaults(func=cmdline_delete)

        parser_kill = subparsers.add_parser("kill", help="kill an experiment")
        parser_kill.add_argument('search_string', nargs='+')
        parser_kill.add_argument('-d', '--delete', action='store_true', help="delete after kill")
        parser_kill.add_argument('-F', '--keep_files', action='store_true', help="keep files on delete")
        parser_kill.set_defaults(func=cmdline_kill)

        parser_rerun = subparsers.add_parser("rerun", help="rerun a config")
        parser_rerun.add_argument('search_string')
        parser_rerun.add_argument('config_id')
        parser_rerun.set_defaults(func=cmdline_rerun)

        parser_abort = subparsers.add_parser("abort", help="abort a config")
        parser_abort.add_argument('search_string')
        parser_abort.add_argument('config_id')
        parser_abort.add_argument('-r', '--rerun', action='store_true', help='rerun the config')
        parser_abort.set_defaults(func=cmdline_abort)

        parser_merge = subparsers.add_parser("merge", help="merge experiments")
        parser_merge.add_argument('search_string', nargs='+')
        parser_merge.set_defaults(func=cmdline_merge)

        parser_kill_all = subparsers.add_parser("killall", help="kill all experiments")
        parser_kill_all.add_argument('-d', '--delete', action='store_true', help="delete after kill")
        parser_kill_all.add_argument('-n', '--name', default=None, help="kill by name")
        parser_kill_all.add_argument('-l', '--limit', type=int, default=0, help="limit")
        parser_kill_all.add_argument('-f', '--force', action='store_true', help="do not confirm")
        parser_kill_all.add_argument('-b', '--batch', type=int, default=0, help="if set, delete in batches of this size")
        parser_kill_all.set_defaults(func=cmdline_kill_all)

        parser_trim = subparsers.add_parser("trim", help="kill and delete enqueued experiments")
        parser_trim.set_defaults(func=cmdline_trim)

        parser_export = subparsers.add_parser("export", help="export an experiment")
        parser_export.add_argument('search_string')
        parser_export.add_argument('-p', '--path', help="the directory to export to", default='.')
        parser_export.add_argument('-m', '--message', help="message to write into the folder", default=None)
        parser_export.add_argument('-c', '--configs', help="comma separated list of configs to export", default=None)
        parser_export.add_argument('-d', '--done', help="export only done configs", action='store_true')
        parser_export.add_argument('-t', '--temp', action='store_true', help='export to a temporary folder')
        parser_export.set_defaults(func=cmdline_export)

        parser_tb = subparsers.add_parser("tb", help="open tensorboard")
        parser_tb.add_argument('search_string', nargs='?', default='latest')
        parser_tb.add_argument('-p', '--port', default=6006, type=int)
        parser_tb.add_argument('-c', '--checkpoints', action='store_true', help="also sync checkpoints")
        parser_tb.add_argument('-i', '--info_only', action='store_true', help="only print common attributes")
        parser_tb.add_argument('-d', '--done', action='store_true', help="only show done configs")
        parser_tb.add_argument('-r', '--running', action='store_true', help="only show running configs")
        parser_tb.set_defaults(func=cmdline_tb)

        parser_tb = subparsers.add_parser("step", help="do hopt step")
        parser_tb.add_argument('search_strings', nargs='*')
        parser_tb.add_argument('-s', '--single', action='store_true', help="force only single step")
        parser_tb.set_defaults(func=cmdline_hopt_step)

        parser_tb = subparsers.add_parser("monitor", help="do hopt monitoring")
        parser_tb.add_argument('search_strings', nargs='*')
        parser_tb.add_argument('-p', '--pause', type=int, default=5, help="seconds to pause")
        parser_tb.set_defaults(func=cmdline_hopt_monitor)

        parser_export = subparsers.add_parser("stats", help="stats about an experiment")
        parser_export.add_argument('search_string')
        parser_export.set_defaults(func=cmdline_stats)

        parser_tb = subparsers.add_parser("clean", help="clean up saved experiments")
        parser_tb.add_argument('-b', '--batch', type=int, default=0, help="if set, delete in batches of this size")
        parser_tb.set_defaults(func=cmdline_clean)


        args = parser.parse_args()

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
