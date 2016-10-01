import pymongo
import os
from rat import utils
import time
from rat.utils import Status
import logging

def maybe_create(d):
    os.makedirs(d, exist_ok=True)
    return d


def get_temp_dir():
    return maybe_create(os.path.expanduser('~/.rattmp'))


def get_experiments_dir():
    return maybe_create(os.path.join(get_temp_dir(), 'experiments'))


def get_experiment_dir(eid):
    return maybe_create(os.path.join(get_experiments_dir(), eid))


def get_config_dir(eid, cid):
    return maybe_create(os.path.join(get_experiment_dir(eid), cid))


def run_config(rat_config, experiment, config):
    logging.info('running experiment {}, config {}'.format(experiment['_id'], config['_id']))
    db, grid = utils.get_mongo(rat_config)

    utils.load_file_tree(grid, get_experiments_dir(), [f[1] for f in config['files']])

    db.experiments.update({'_id': experiment['_id'], 'configs._id': config['_id']}, {'$set': {'configs.$.status': Status.running}})
    main_file = config['spec']['main_file']
    config_dir = get_config_dir(experiment['_id'], config['_id'])
    os.chdir(config_dir)
    start_time = time.time()
    utils.system_call('python3 {} > stdout.txt 2> stderr.txt'.format(main_file))

    all_files = utils.get_all_files(config_dir)
    new_fns = [fn for fn in all_files if os.path.getmtime(fn) > start_time]
    new_fids = utils.save_file_tree(grid, config_dir, new_fns, experiment['_id'], config['_id'])

    db.experiments.update({'_id': experiment['_id'], 'configs._id': config['_id']}, {'$set': {'configs.$.status': Status.done, 'configs.$.resultfiles': list(zip(new_fns, new_fids))}})
    db.experiments.update({'_id': experiment['_id'], 'configs.status': {'$not': {'$elemMatch': {'$ne': Status.done}}}}, {'$set': {'status': Status.done}})


    
