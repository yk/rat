#!/usr/bin/env python3

from rat import worker
from rat import utils
import confprod
import logging


class HyperoptStrategy:
    def __init__(self, experiment, state, spec):
        self.experiment = experiment
        self.state = state
        self.spec = spec

    def get_next_config(self):
        config = confprod.generate_configurations(self.spec, 1)
        return config

def build_hyperopt_strategy(definition, experiment, state, spec):
    return HyperoptStrategy(experiment, state, spec)


class HyperoptWorker(worker.ConditionTermWorker):
    pass


def do_hyperopt(db, grid, hyperopt_id):
    hopt = db.hyperopt.find_one(hyperopt_id)
    exp = db.experiments.find_one(hopt['experiment_id'])

    configs_in_queue = len([c for c in exp['configs'] if c['status'] <= utils.Status.running])

    if configs_in_queue >= hopt['queue_size']:
        return

    search_strategy = build_hyperopt_strategy(hopt['search_strategy'], exp, hopt['state'], hopt['spec'])

    config = search_strategy.get_next_config()

    db.hyperopt.update(hyperopt_id, {'$set': {'state': search_strategy.state}})

    if config:
        logging.info('scheduling next config %s', config)
        from rat import rat
        new_fids = utils.duplicate_files(grid, hopt['files'])
        rat.run_config(exp, get_free_config_id(exp), config, new_fids)
