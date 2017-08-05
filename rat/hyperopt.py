#!/usr/bin/env python3

from glob import glob
import os
from rat import worker
from rat import utils
from rat.utils import Status, SearchStatus
import logging
import tempfile
import numpy as np
import itertools as itt
import time


def read_tfevents(fn):
    import tensorflow as tf
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
    def needs_export(self):
        return False

    def extract(self, config, *args):
        return {}


class ValueExtractor(Extractor):
    def __init__(self, keys):
        self.keys = keys

    def extract(self, config, *args):
        v = {}
        for k in self.keys:
            v[k] = config.get(k, None)
        return v

class ExportExtractor(Extractor):
    def needs_export(self):
        return True

class SummaryScalarExtractor(ExportExtractor):
    def __init__(self, keys, average_over=3):
        self.keys = keys
        self.average_over = average_over

    def extract(self, config, path, *args):
        evts = read_tfevents(glob(os.path.join(path, 'logs') + '/*.tfevents.*')[0])
        v = {}
        for k in self.keys:
            _, vs = extract_tfevent_scalar(evts, k)
            v[k] = np.mean(vs[-self.average_over:])
        return v

class Scorer:
    def score(self, extracted):
        return 1.

class SimpleValueScorer(Scorer):
    def __init__(self, key, lower_is_better=False):
        self.key = key
        self.lower_is_better = lower_is_better
    
    def score(self, extracted):
        s = extracted.get(self.key, -np.inf)
        if self.lower_is_better:
            s = -s
        return s


class HyperoptStrategyBase:
    def __init__(self, args, experiment, state, spec, history):
        self.args = args
        self.experiment = experiment
        self.state = state
        self.spec = spec
        self.history = history

    def get_running_or_done_specs(self):
        return list(set([c['spec'] for c in self.experiment['configs']] + [h['spec'] for h in self.history]))

    def get_next_config(self):
        idx = self.state.get('idx', 0)
        if idx < len(self.spec):
            c = self.spec[idx]
            self.state['idx'] = idx + 1
            return c

    def is_done(self):
        return self.state.get('idx', 0) >= len(self.spec)
            

    def get_extractors(self):
        return [
                ValueExtractor(['status', 'start_time', 'end_time']),
                ]

    def get_scorers(self):
        return [
                Scorer(),
                ]

    def score(self, extracted):
        for s in self.get_scorers()[::-1]:
            ss = s.score(extracted)
            if ss is not None:
                return ss

    def extract(self, exp, config):
        values = {}
        extractors = self.get_extractors()
        if any([e.needs_export() for e in extractors]):
            from rat import rat
            with tempfile.TemporaryDirectory() as path:
                epat, _ = utils.exclude_include_patterns(config['spec'])
                epat.append('.ckpt')
                epat.append('ext/')
                rat.export_config(exp, config, path, exclude_patterns=epat)
                for e in extractors:
                    values.update(e.extract(config, path))
        else:
            for e in extractors:
                values.update(e.extract(config))
        return values

class ProdCreateStrategy:
    def __init__(self, args, experiment, state, spec, history):
        import confprod
        if 'spec' not in state:
            state['spec'] = confprod.generate_configurations(spec)
        super().__init__(args, experiment, state, state['spec'], history)

class SampleCreateStrategy:
    def __init__(self, args, experiment, state, spec, history):
        import confprod
        sample_size = args.get('sample_size', -1)
        if 'spec' not in state:
            state['spec'] = confprod.generate_configurations(spec, sample_size)
        super().__init__(args, experiment, state, state['spec'], history)


class SummaryScalarStrategy:
    def get_extractors(self):
        return [SummaryScalarExtractor([self.args['score_key']])]

    def get_scorers(self):
        return [SimpleValueScorer(self.args['score_key'], self.args.get('lower_is_better', False))]



def build_hyperopt_strategy(exp):
    definition, state, spec, history = exp['search_strategy'], exp['state'], exp['spec'], exp['history']
    bases = [HyperoptStrategyBase]
    create = definition.get('create', 'raw')
    if create == 'raw':
        pass
    elif create == 'prod':
        bases.append(ProdCreateStrategy)
    elif create == 'sample':
        bases.append(SampleCreateStrategy)

    score = definition.get('score')
    if score == 'summary_scalar':
        bases.append(SummaryScalarStrategy)
    elif score == 'constant':
        pass
    class HyperoptStrategy(*bases[::-1]):
        pass
    strategy = HyperoptStrategy(definition.get('args', {}), exp, state, spec, history)
    return strategy


class HyperoptWorker(worker.ConditionTermWorker):
    pass




def do_hyperopt_steps(experiment_id):
    did_step = True
    while did_step:
        did_step = do_hyperopt_step(experiment_id)
        time.sleep(0.5)


def do_hyperopt_step(exp_id):
    logging.root.setLevel(logging.INFO)
    from rat import rat

    exp = rat.find_experiment(exp_id)

    search_status = exp.get('search_status', SearchStatus.done)
    if  search_status >= SearchStatus.done:
        return False

    configs_in_queue = len([c for c in exp['configs'] if c['status'] < Status.running])
    configs_in_queue_or_running = len([c for c in exp['configs'] if c['status'] <= Status.running])

    if configs_in_queue >= exp['search_strategy'].get('queue_size', configs_in_queue + 1):
        logging.info('queue full')
        return

    search_strategy = build_hyperopt_strategy(exp)

    new_done = [c for c in exp['configs'] if c['status'] >= Status.done and c['_id'] not in [h['_id'] for h in exp['history']]]

    for ndc in new_done:
        hist_vals = search_strategy.extract(exp, ndc)
        hist_vals['_id'] = ndc['_id']
        hist_vals['spec'] = ndc['spec']
        logging.info('adding %s to history', hist_vals)
        logging.info('score = %f', search_strategy.score(hist_vals))
        exp['history'].append(hist_vals)
        rat.db.experiments.update({'_id': exp['_id']}, {'$push': {'history': hist_vals}})

    keep_best = exp['search_strategy'].get('keep_best', -1)
    if keep_best > 0:
        best_ids = [h['_id'] for h in itt.islice(sorted(exp['history'], key=lambda h: search_strategy.score(h), reverse=True), keep_best)]

        for c in exp['configs']:
            if c['status'] >= Status.done and c['_id'] not in best_ids:
                logging.info('deleting config %s because it is not best', c)
                rat.delete_config(exp, c)

    if search_strategy.is_done() and configs_in_queue_or_running == 0:
        exp['search_status'] = SearchStatus.done
        rat.db.experiments.update({'_id': exp['_id']}, {'$set': {'search_status': exp['search_status']}})
        config = None
    else:
        if search_status == SearchStatus.enqueued:
            exp['search_status'] = SearchStatus.running
            rat.db.experiments.update({'_id': exp['_id']}, {'$set': {'search_status': exp['search_status']}})
        config = search_strategy.get_next_config()

    rat.db.experiments.update({'_id': exp['_id']}, {'$set': {'state': search_strategy.state}})

    if config:
        logging.info('scheduling next config %s', config)
        rat.run_config(exp, rat.get_free_config_id(exp), config)
        return True
    else:
        return False
