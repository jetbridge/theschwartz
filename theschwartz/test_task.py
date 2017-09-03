"""Dumb test worker."""

from .task import Task
import logging
import time
import random
import threading

log = logging.getLogger(__name__)


class TestTask(Task):
    """Pretends to do work - just hits the database."""

    TASK_TYPE = 'theschwartz_test'

    __mapper_args__ = {
        'polymorphic_identity': TASK_TYPE
    }

    def do_work(self):
        """Simulate doing some work."""            
        environment = self.task_args['environment']
        if environment is 'Production':
            raise Exception("😒")
        did_work = self.perform_query()
        log.debug(f"🏃  Ran one loop on thread {threading.get_ident()}")
        return did_work

    def perform_query(self):
        """Do a long-running query."""
        user = User.query.first()
        user.reset_password_token = time.time()
        db.session.commit()
        sleep_time = random.randint(1, 10) / 10
        db.engine.execute(f'BEGIN; SELECT pg_sleep({sleep_time}); ROLLBACK;')
        return True
