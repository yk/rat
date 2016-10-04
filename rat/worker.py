import pymongo
import os
from rat import utils
import time
from rat.utils import Status
import logging
import tempfile
import signal
from rq import Worker


class TermWorker(Worker):

    def request_force_force_stop(self, signum, frame):
        """Terminates the application (cold shutdown)."""
        self.log.warning('Cold shut down')

        if self.horse_pid:
            msg = 'Taking down horse {0} with me'.format(self.horse_pid)
            self.log.debug(msg)
            try:
                os.kill(self.horse_pid, signal.SIGKILL)
            except OSError as e:
                if e.errno != errno.ESRCH:
                    self.log.debug('Horse already down')
                    raise
        raise SystemExit()

    def request_force_stop(self, signum, frame):
        """Terminates the application (semi cold shutdown)."""
        self.log.warning('Semi cold shut down')

        signal.signal(signal.SIGTERM, self.request_force_force_stop)

        if self.horse_pid:
            msg = 'Semi Taking down horse {0} with me'.format(self.horse_pid)
            self.log.debug(msg)
            try:
                os.kill(self.horse_pid, signal.SIGTERM)
                os.waitpid(self.horse_pid, 0)
            except ChildProcessError:
                pass


def run_config(rat_config, experiment, config):
    host = os.environ.get('RAT_HOST', '')
    db, grid = utils.get_mongo(rat_config)
    logging.info('running experiment {}, config {}'.format(experiment['_id'], config['_id']))
    with tempfile.TemporaryDirectory() as path:
        os.chdir(path)

        utils.load_file_tree(grid, path, config['files'])

        db.experiments.update({'_id': experiment['_id'], 'configs._id': config['_id']}, {'$set': {'configs.$.status': Status.running, 'status': Status.running, 'configs.$.host': host, 'configs.$.path': path}})
        main_file = config['spec']['main_file']
        start_time = time.time()
        # utils.system_call('python3 {} > stdout.txt 2> stderr.txt'.format(main_file))
        flags = utils.dict_to_flags(config['spec'])
        process = utils.async_system_call('python3 {} {}'.format(main_file, flags))
        def handler(signum, frame):
            process.terminate()
        signal.signal(signal.SIGTERM, handler)
        try:
            process.wait()
        except:
            # process.kill()
            process.terminate()

        all_files = utils.get_all_files(path)
        new_fns = [fn for fn in all_files if os.path.getmtime(fn) > start_time]
        new_fids = utils.save_file_tree(grid, path, new_fns)

        db.experiments.update({'_id': experiment['_id'], 'configs._id': config['_id']}, {'$set': {'configs.$.status': Status.done, 'configs.$.resultfiles': new_fids}})
        db.experiments.update({'_id': experiment['_id'], 'configs.status': {'$not': {'$elemMatch': {'$ne': Status.done}}}}, {'$set': {'status': Status.done}})


    
