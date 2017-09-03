"""Database-backed task model."""

import logging
from sqlalchemy import or_, func, Column, Integer, DateTime, Boolean, Index, Enum
from sqlalchemy.orm.query import Query
from sqlalchemy.sql.sqltypes import Text
from sqlalchemy.dialects.postgresql import JSONB

TSTZ = DateTime(timezone=True)
log = logging.getLogger(__name__)


class Task:
    """Base task table holding completed and not-completed tasks.

    Inherit from TrackingTask with polymorphic single-table inheritance to add more columns.
    """

    __tablename__ = 'theschwartz_task'

    # configuration
    MAX_RETRY_COUNT = 5  # we retry executing a task this many times
    GRAB_TIME_IN_MINUTES = 100  # the absolute very longest a task will ever take to complete
    DELETE_ON_COMPLETION = False

    id = Column(Integer, primary_key=True)
    created = Column(TSTZ, nullable=False, server_default=func.now())
    is_completed = Column(Boolean, nullable=False, server_default='f')
    retry_count = Column(Integer, server_default='0', nullable=False)
    scheduled_execution_date = Column(TSTZ, nullable=False, server_default=func.now())
    grabbed_until = Column(TSTZ, nullable=True)
    last_execution_start_date = Column(TSTZ, nullable=True)
    last_execution_end_date = Column(TSTZ, nullable=True)
    priority = Column(Integer, server_default='0', nullable=False)  # higher number = higher priority

    task_type = Column(Text, nullable=False)
    task_args = Column(JSONB)  # json or pickle?

    # indexes used by task fetching queries
    task_completed_index = Index('theschwartz_task_completed_index', is_completed)
    retry_count_index = Index('theschwartz_task_retry_count', retry_count)
    scheduled_execution_date_index = Index('theschwartz_task_scheduled_execution_date', scheduled_execution_date)

    # task type disciminator column
    __mapper_args__ = {
        'polymorphic_on': task_type,
        'polymorphic_identity': 'task'
    }

    def do_work(self):
        """Your task subclasses will implement this."""
        raise NotImplementedException(f"do_work is not defined for {self.__class__.name}")

    def log_heartbeat(self, did_work):
        """Log the successful end of a job cycle for monitoring purposes."""
        # make sure that "func.now()" is from this point (new transaction) and not skewed from the last transaction's start time
        db.session.commit()
        # update heartbeat
        hostname = platform.node()
        insert_stmt = pg_insert(Heartbeat).values(
            hostname=hostname,
            updated=func.now(),
            task_type=self.task_type,
        )
        do_update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['task_type'],
            set_=dict(updated=func.now(), hostname=hostname)
        )
        db.session.execute(do_update_stmt)
        # db.session.add(hb)
        db.session.commit()


class Heartbeat:
    """A row indicates a heartbeat has been recorded for a given task_type/environment which signifies that the process was alive at the time."""

    __tablename__ = 'theschwartz_heartbeat'

    id = Column('id', Integer(), primary_key=True)
    created = Column(TSTZ, nullable=False, server_default=func.now())
    hostname = Column(Text, nullable=True)
    task_type = Column(Text, nullable=False, unique=True)
    updated = Column(TSTZ, nullable=False, server_default=func.now())


class TaskQuery(Query):
    """Query class for tasks."""

    BATCH_SIZE = 10  # how many jobs to grab at once

    def needs_work(self):
        """Get jobs that are not yet completed, under max retry count, not grabbed, and scheduled for <=now."""
        return self \
            .is_not_completed() \
            .can_retry() \
            .not_grabbed() \
            .scheduled_for_now()

    def priority_order(self):
        """Highest priority jobs first."""
        return self.order_by(Task.priority.desc())

    def not_locked(self):
        """Lock selected rows for duration of current transaction and don't grab currently-locked rows."""
        return self.with_for_update(skip_locked=True, of=Task)

    def not_grabbed(self):
        """Get tasks that are not marked as being worked on.

        After a row is locked with the not_locked FOR UPDATE clause, we mark it as grabbed because the FOR UPDATE modifier only lasts for the duration of the transaction.
        """
        return self.filter(or_(Task.grabbed_until < func.now(), Task.grabbed_until.is_(None)))

    def is_not_completed(self):
        """Not marked as completed."""
        return self.filter(Task.is_completed.isnot(True))

    def can_retry(self):
        """Retry count is less than maximum."""
        return self.filter(Task.retry_count < Task.MAX_RETRY_COUNT)

    def scheduled_for_now(self):
        """Get tasks scheduled to be worked on in the past or present."""
        return self.filter(Task.scheduled_execution_date <= func.now())

    def grabbed_until_date(self):
        """Return due date of task.
        
        It must be completed by this date or it will get retried.
        This prevents a worker crash from making jobs impossible to be completed.
        """
        return text(f"NOW() + INTERVAL '{self.GRAB_TIME_IN_MINUTES} MINUTES'")

    def bump_retry_count(self, session=None):
        """Increase retry count by one if new retry count is greater then MAX_RETRY_COUNT."""
        if not session:
            session = db.session
        self.retry_count += 1
        if self.retry_count >= self.MAX_RETRY_COUNT:
            log.error("Failed to execute task {}".format(self))
        session.commit()
