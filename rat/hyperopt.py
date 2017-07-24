#!/usr/bin/env python3

from glob import glob
import os
from rat import worker
from rat import utils
from rat.utils import Status
import confprod
import logging
import tempfile
import tensorflow as tf
import numpy as np


def read_tfevents(fn):
    s = list(tf.train.summary_iterator(fn))[1:]
    return s


def extract_tfevent_values(summaries, tag, get_val_fn):
    steps = []
    values = []
    for s in summaries:
        for v in s.summary.value:
            if v.tag == tag:
                steps.append(s.step)
                values.append(get_val_fn(v))
    return steps, values


def extract_tfevent_scalar(summaries, tag):
    return extract_tfevent_values(summaries, tag, lambda v: v.simple_value)


def extract_tfevent_image(summaries, tag):
    return extract_tfevent_values(summaries, tag, lambda v: v.image.encoded_image_string)



class Extractor:
    def extract(self, config, path):
        return {}


class ValueExtractor(Extractor):
    def __init__(self, keys):
        self.keys = keys

    def extract(self, config, path):
        v = {}
        for k in self.keys:
            v[k] = config.get(k, None)
        return v

class SummaryScalarExtractor(Extractor):
    def __init__(self, keys, average_over=3):
        self.keys = keys
        self.average_over = average_over

    def extract(self, config, path):
        evts = read_tfevents(glob(os.path.join(path, 'logs') + '/*.tfevents.*')[0])
        v = {}
        for k in self.keys:
            vs = extract_tfevent_scalar(evts, k)
            v[k] = np.mean(vs[-self.average_over:])
        return v


class HyperoptStrategyBase:
    def __init__(self, args, experiment, state, spec):
        self.args = args
        self.experiment = experiment
        self.state = state
        self.spec = spec

    def get_next_config(self):
        for _ in range(100):
            config = confprod.generate_configurations(self.spec, 1)[0]
            if self.args.get('reschedule', True):
                return config
            if config not in [c['spec'] for c in self.experiment['configs']]:
                return config
        logging.info('cant find new config')

    def get_extractors(self):
        return [
                ValueExtractor(['status', 'start_time', 'end_time']),
                SummaryScalarExtractor(['c_loss']),
                ]

    def extract(self, config):
        from rat import rat
        values = {}
        with tempfile.TemporaryDirectory() as path:
            epat, _ = utils.exclude_include_patterns(config['spec'])
            epat.append('.ckpt')
            epat.append('ext/')
            rat.export_config(config, path, exclude_patterns=epat)
            for e in self.get_extractors():
                values.update(e.extract(config, path))
        return values



def build_hyperopt_strategy(definition, experiment, state, spec):
    class HyperoptStrategy(HyperoptStrategyBase):
        pass
    return HyperoptStrategy(definition, experiment, state, spec)


class HyperoptWorker(worker.ConditionTermWorker):
    pass


def do_hyperopt_step(hopt):
    logging.root.setLevel(logging.INFO)
    from rat import rat
    exp = rat.get_experiment_for_hyperopt(hopt)

    configs_in_queue = len([c for c in exp['configs'] if c['status'] < Status.running])

    if configs_in_queue >= hopt['queue_size']:
        return

    search_strategy = build_hyperopt_strategy(hopt['search_strategy'], exp, hopt['state'], hopt['spec'])

    # new_done = [c for c in exp['configs'] if c['status'] >= Status.done and c['_id'] not in hopt['history']]
    new_done = [c for c in exp['configs'] if c['status'] >= Status.done]

    for ndc in new_done:
        hist_vals = search_strategy.extract(ndc)
        hist_vals['_id'] = ndc['_id']
        logging.info('adding %s to history', hist_vals)
        hopt['history'].append(hist_vals)
        rat.db.hyperopt.update({'_id': hopt['_id']}, {'$push': {'history': hist_vals}})

    config = search_strategy.get_next_config()

    rat.db.hyperopt.update({'_id': hopt['_id']}, {'$set': {'state': search_strategy.state}})

    if config:
        logging.info('scheduling next config %s', config)
        new_fids = utils.duplicate_files(rat.grid, hopt['files'])
        rat.run_config(exp, rat.get_free_config_id(exp), config, new_fids)
