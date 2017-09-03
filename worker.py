"""Asynchronous job queue worker."""

import logging
import time
from jetbridge import app

log = logging.getLogger(__name__)


class Worker:
    """Worker mainloop and job wrappers."""

    IDLE_SLEEP_TIME = 2  # sleep after looking for work and finding none


    def do_work(self):
        """Do one cycle of work.

        Each job type should grab one job and work on it here.
        """
        did_work = False
        for worker in self.workers:
            did_work = worker.do_one_loop() or did_work
            worker.log_heartbeat(did_work)
            if self.cancelled:
                return

        if not did_work:
            # chill for a sec?
            time.sleep(self.IDLE_SLEEP_TIME)

    def stop_work(self):
        """Return when current job is finished."""
        self.cancelled = True



    def begin_work(self, worker_id):
        """Thread entry point."""
        worker = JetBridgeWorker(test_worker=test_worker)
        workers.append(worker)
        log.info(f"👷  Starting worker {worker_id}")
        worker.main_work_loop()


    def stop_work(self, executor):
        """Tell workers to finish up."""
        log.info("Workers aborted. finishing up... 🖐")
        for worker in workers:
            worker.stop_work()
        executor.shutdown(wait=False)


    def start(self, thread_count=5):
        """Run multiple workers in parallel."""
        worker_count = int(app.config.get('WORKER_COUNT'))
        assert(worker_count)

        # spawn threads
        executor = ThreadPoolExecutor(max_workers=worker_count)
        for i in range(worker_count):
            executor.submit(begin_work, worker_id=i)

        # clean up nicely when exiting
        atexit.register(stop_work, executor)

        # workers started now
        # run forever
        while True:
            # yield time up
            time.sleep(1)


if __name__ == '__main__':
    worker = Worker()
    worker.start()
