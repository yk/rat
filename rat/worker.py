import pymongo
import os
from rat import utils
import time
from rat.utils import Status
import logging
import tempfile
import signal
from rq import Worker
from rq.logutils import setup_loghandlers
from rq.version import VERSION
from rq.worker import WorkerStatus
from rq.worker import StopRequested
import sh
import sys
import select
import fcntl

# logging.root.setLevel(logging.DEBUG)

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



class ConditionTermWorker(TermWorker):
    SLEEP_TIME = 3
    def ready_to_work(self):
        raise Exception("Not Implemented")

    def after_execute(self):
        pass

    def work(self, burst=False, logging_level="INFO", **kwargs):
        setup_loghandlers(logging_level)
        self._install_signal_handlers()

        did_perform_work = False
        self.register_birth()
        self.log.info("RQ worker {0!r} started, version {1}".format(self.key, VERSION))
        self.set_state(WorkerStatus.STARTED)

        try:
            while True:
                self.heartbeat()
                if self.ready_to_work():
                    try:
                        self.check_for_suspension(burst)

                        if self.should_run_maintenance_tasks:
                            self.clean_registries()

                        if self._stop_requested:
                            self.log.info('Stopping on request')
                            break

                        timeout = None if burst else max(1, self.default_worker_ttl - 60)

                        result = self.dequeue_job_and_maintain_ttl(timeout)
                        if result is None:
                            if burst:
                                self.log.info("RQ worker {0!r} done, quitting".format(self.key))
                            break
                    except StopRequested:
                        break

                    job, queue = result
                    if self.ready_to_work():
                        self.execute_job(job, queue)
                        self.heartbeat()
                        did_perform_work = True
                        self.after_execute()
                    else:
                        self.heartbeat()
                        self.log.info("not ready to work, requeueing job")
                        queue.enqueue_job(job)
                        time.sleep(ConditionTermWorker.SLEEP_TIME)

                else:
                    self.heartbeat()
                    self.log.info("not ready to work, sleeping")
                    time.sleep(ConditionTermWorker.SLEEP_TIME)

        finally:
            if not self.is_horse:
                self.register_death()
        return did_perform_work




class GpuWorker(ConditionTermWorker):
    def after_execute(self):
        import pycuda.driver as pd
        pd.init()
        cc = pd.Context.get_current()
        if cc is not None:
            cc.detach()
        time.sleep(1)

    def ready_to_work(self):
        # import pycuda.driver as pd
        gpu = os.environ['CUDA_VISIBLE_DEVICES']
        if not gpu or gpu == '':
            return False
        try:
            nvsmi = sh.Command('nvidia-smi')
            ctext = nvsmi()
            _, pblock, ublock = ctext.split('|==')
            ublock = ublock.split('+--')[0].split('\n')[1:-1]
            pids = [u.split()[1:3] for u in ublock if not ('No running' in u)]
            gids, pids = zip(*pids) if len(pids) > 0 else ([], [])
            for gid in gids:
                if gid == gpu:
                    return False
            return True
        except:
            return False
        # try:
            # self.log.info('1')
            # self.log.info('2')
            # # ctx = pt.make_default_context()
            # ctx = pd.Device(0).make_context(pd.ctx_flags.BLOCKING_SYNC)
            # self.log.info('3')
            # ctx.detach()
            # self.log.info('4')
            # return True
        # except:
            # c = pd.Context.get_current()
            # if c is not None:
                # c.detach()
                # time.sleep(1)
            # return False


def run_config(rat_config, experiment, config):
    host = os.environ.get('RAT_HOST', '')
    db, grid = utils.get_mongo(rat_config)
    logging.info('running experiment {}, config {}'.format(experiment['_id'], config['_id']))
    with tempfile.TemporaryDirectory() as path:
        os.chdir(path)

        utils.load_file_tree(grid, path, experiment['files'])

        start_time = time.time()
        db.experiments.update({'_id': experiment['_id'], 'configs._id': config['_id']}, {'$set': {'configs.$.status': Status.running, 'status': Status.running, 'configs.$.host': host, 'configs.$.path': path, 'configs.$.start_time': start_time}})
        main_file = experiment['main_file']
        run_cmd = experiment.get('command', 'python3')
        # utils.system_call('python3 {} > stdout.txt 2> stderr.txt'.format(main_file))
        flags = utils.dict_to_flags(config['spec'])
        cmd = '{} {} {}'.format(run_cmd, main_file, flags)
        logging.info(cmd)
        cmd_log_path = os.path.join(path, 'cmdlog.txt')
        with open(cmd_log_path, 'w') as f:
            f.write(cmd)
        process = utils.async_system_call(cmd)

        def handler(signum, frame):
            process.terminate()

        def non_block_read(instream, outstream, outfile):
            fd = instream.fileno()
            fl = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
            try:
                while True:
                    l = instream.read(1024)
                    if l is not None and len(l) > 0:
                        l = l.decode()
                        outstream.write(l)
                        outfile.write(l)
                    else:
                        time.sleep(.1)
                        break
            except Exception as e:
                logging.warn(e)
                pass

        signal.signal(signal.SIGTERM, handler)
        stdout_path = os.path.join(path, 'stdout.txt')
        stderr_path = os.path.join(path, 'stderr.txt')
        with open(stdout_path, 'w') as stdoutf, open(stderr_path, 'w') as stderrf:
            try:
                while process.poll() is None:
                    non_block_read(process.stdout, sys.stdout, stdoutf)
                    non_block_read(process.stderr, sys.stderr, stderrf)

                non_block_read(process.stdout, sys.stdout, stdoutf)
                non_block_read(process.stderr, sys.stderr, stderrf)

                # process.wait()
            except Exception as e:
                logging.warning(e)
                # process.kill()
                process.terminate()

        end_time = time.time()

        all_files = utils.get_all_files(path)
        new_fns = [fn for fn in all_files if os.path.getmtime(fn) >= start_time - 1.]
        # new_fids = utils.save_file_tree(grid, path, new_fns, exclude_patterns=['latest'])
        new_fids = utils.save_file_tree(grid, path, new_fns, exclude_patterns=[])

        db.experiments.update({'_id': experiment['_id'], 'configs._id': config['_id']}, {'$set': {'configs.$.status': Status.done, 'configs.$.resultfiles': new_fids, 'configs.$.end_time': end_time}})
        db.experiments.update({'_id': experiment['_id'], 'configs': {'$not': {'$elemMatch': {'status': {'$ne': Status.done}}}}}, {'$set': {'status': Status.done, 'end_time': end_time}})
