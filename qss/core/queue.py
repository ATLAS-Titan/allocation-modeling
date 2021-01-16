#
# Copyright European Organization for Nuclear Research (CERN)
#           National Research Centre "Kurchatov Institute"
#           Rutgers University
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Author(s):
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2017-2018
#

from collections import defaultdict
from itertools import islice

from .constants import QueueDiscipline


def fifo_queue_append(queue, element):
    queue.append(element)


def priority_queue_append(queue, element):
    element_idx = 0
    for idx in xrange(len(queue) - 1, -1, -1):
        if queue[idx].priority >= element.priority:
            element_idx = idx + 1
            break
    queue.insert(element_idx, element)


class QueueManager(object):

    def __init__(self, policy=None, limit=None, with_buffer=False):
        """
        Initialization (limits are applied to the queue, excluding the buffer).

        @param policy: Policy for the queue behaviour.
        @type policy: dict/None
        @param limit: Maximum (total) number of jobs in the queue.
        @type limit: int/None
        @param with_buffer: Flag to use the buffer (instead of dropping jobs).
        @type with_buffer: bool
        """
        self.__queue = []
        self.__latest_queued_timestamp = 0.
        self.__queued_buffer_job = None

        self.__num_jobs_per_source = defaultdict(int)

        policy = policy or {}

        # queue limits
        self.__limits = policy.get('limit', {})
        if limit:
            self.__limits['_total'] = limit

        # queue discipline
        self.__discipline = policy.get('discipline', QueueDiscipline.FIFO)
        if self.__discipline == QueueDiscipline.FIFO:
            self.__queue_append = fifo_queue_append
        elif self.__discipline == QueueDiscipline.Priority:
            self.__queue_append = priority_queue_append
        else:
            raise Exception('Queue discipline is unknown.')

        # set method for initialization of the new job in the queue
        if policy.get('job_init'):
            self.__job_init = policy['job_init']
        else:
            def job_init_dummy(job):
                pass
            self.__job_init = job_init_dummy

        # buffer set up
        if with_buffer:
            self.__buffer = defaultdict(list)
            self.__num_dropped = None
        else:
            self.__buffer = None
            self.__num_dropped = defaultdict(int, {'_total': 0})

    def reset(self):
        """
        Reset parameters.
        """
        del self.__queue[:]
        self.__latest_queued_timestamp = 0.
        self.__queued_buffer_job = None

        self.__num_jobs_per_source.clear()

        if self.__buffer is not None:
            for source in self.__buffer:
                del self.__buffer[source][:]

        if self.__num_dropped is not None:
            for source in self.__num_dropped:
                self.__num_dropped[source] = 0

    @property
    def is_empty(self):
        """
        Flag shows whether queue is empty or not.

        @return: Flag that queue is empty.
        @rtype: bool
        """
        return True if not self.__queue else False

    @property
    def length(self):
        """
        Get the number of all jobs in the queue.

        @return: Number of jobs.
        @rtype: int
        """
        return len(self.__queue)

    @property
    def length_buffer(self):
        """
        Get the number of all jobs in the buffer.

        @return: Number of jobs.
        @rtype: int
        """
        output = 0

        if self.__buffer is not None:
            output = sum(map(lambda x: len(x), self.__buffer.values()))

        return output

    @property
    def length_total(self):
        """
        Get the number of all jobs in the queue and in the buffer.

        @return: Number of jobs.
        @rtype: int
        """
        return self.length + self.length_buffer

    def get_num_jobs_per_source(self, source, in_buffer=False):
        """
        Get the number of jobs in the queue/buffer by the source name.

        @param source: Source name of the job.
        @type source: str
        @param in_buffer: Flag to count jobs in the buffer.
        @type in_buffer: bool
        @return: Number of jobs.
        @rtype: int
        """
        if not in_buffer:
            output = self.__num_jobs_per_source[source]
        else:
            output = 0
            if self.__buffer is not None and source in self.__buffer:
                output = len(self.__buffer[source])

        return output

    def get_num_jobs_with_source_names(self, in_buffer=False):
        """
        Get the number of jobs with corresponding source names.

        @param in_buffer: Flag to count jobs in the buffer.
        @type in_buffer: bool
        @return: Pairs of source names and the corresponding number of jobs.
        @rtype: list((str, int))
        """
        if not in_buffer:
            output = self.__num_jobs_per_source.items()
        else:
            output = []
            if self.__buffer is not None:
                output = map(lambda (k, v): (k, len(v)), self.__buffer.items())

        return output

    def __increase_num_jobs_per_source(self, source):
        """
        Increase the number of jobs (in the queue) from the specific source.

        @param source: Source name of the job.
        @type source: str
        """
        self.__num_jobs_per_source[source] += 1

    def __decrease_num_jobs_per_source(self, source):
        """
        Decrease the number of jobs (in the queue) from the specific source.

        @param source: Source name of the job.
        @type source: str
        """
        if source in self.__num_jobs_per_source:
            self.__num_jobs_per_source[source] -= 1

            if not self.__num_jobs_per_source[source]:
                del self.__num_jobs_per_source[source]

    @property
    def num_dropped(self):
        """
        Get the number of all dropped jobs.

        @return: Number of dropped jobs.
        @rtype: int
        """
        return 0 if self.__num_dropped is None else self.__num_dropped['_total']

    def get_num_dropped_per_source(self, source):
        """
        Get the number of dropped jobs per the source name.

        @param source: Source name of the job.
        @type source: str
        @return: Number of dropped jobs.
        @rtype: int
        """
        return 0 if self.__num_dropped is None else self.__num_dropped[source]

    def get_num_dropped_with_source_names(self):
        """
        Get the number of dropped jobs with corresponding source names.

        @return: Pairs of source names and corresponding number of dropped jobs.
        @rtype: list((str, int))
        """
        return [] if self.__num_dropped is None else self.__num_dropped.items()

    def __process_approved_job(self, job, current_time):
        """
        Process element that is approved to be added to the queue.

        @param job: Job object.
        @type job: qss.core.job.Job
        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        """
        if self.__discipline == QueueDiscipline.Priority:
            time_delta = current_time - self.__latest_queued_timestamp
            for element in self.iterator():
                element.increase_priority(value=time_delta)

        self.__job_init(job=job)
        self.__queue_append(queue=self.__queue, element=job)
        self.__increase_num_jobs_per_source(source=job.source)
        self.__latest_queued_timestamp = current_time

    def __process_rejected_job(self, job):
        """
        Process element that was not added to the queue (due to the limit).

        @param job: Job object.
        @type job: qss.core.job.Job
        """
        if self.__buffer is not None:
            self.__buffer[job.source].append(job)

        elif self.__num_dropped is not None:
            self.__num_dropped[job.source] += 1
            self.__num_dropped['_total'] += 1

    def __post_pop_job_per_source(self, source, current_time):
        """
        Actions after the next job is taken (pulled) from the queue.

        @param source: Source name of the job.
        @type source: str
        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        """
        self.__decrease_num_jobs_per_source(source=source)

        # get the job (of the defined source name) from the buffer
        if self.get_num_jobs_per_source(source=source, in_buffer=True):
            job_from_buffer = self.__buffer[source].pop(0)
            self.add(job=job_from_buffer, current_time=current_time)
            self.__queued_buffer_job = job_from_buffer

            if not self.__buffer[source]:
                del self.__buffer[source]

    def add(self, job, current_time):
        """
        Add element (job) to the queue.

        @param job: Job object.
        @type job: qss.core.job.Job
        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        @return: Status code (0 - success, 1 - rejected).
        @rtype: int
        """
        output = 0

        with_limit, has_free_spots = False, True

        if '_total' in self.__limits:
            if (self.__limits['_total'] - self.length) < 1:
                has_free_spots = False
            with_limit = True

        if job.source in self.__limits:
            source_limit_key = job.source
        elif '_per_source' in self.__limits:
            source_limit_key = '_per_source'
        else:
            source_limit_key = None

        if has_free_spots and source_limit_key:
            if (self.__limits[source_limit_key] -
                    self.get_num_jobs_per_source(source=job.source)) < 1:
                has_free_spots = False
            with_limit = True

        if not with_limit or has_free_spots:
            self.__process_approved_job(job=job, current_time=current_time)
        elif with_limit:
            self.__process_rejected_job(job=job)
            output = 1

        return output

    def show_last(self):
        """
        Show the last job in the queue (without removing it from the queue).

        @return: Job object.
        @rtype: qss.core.job.Job
        """
        return self.__queue[-1]

    def show_next(self):
        """
        Show the next available job without removing it from the queue.

        @return: Job object.
        @rtype: qss.core.job.Job
        """
        return self.__queue[0]

    def get_new_added_from_buffer(self, current_time):
        """
        Show the new job that was added at this (current) time.

        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        @return: Job object.
        @rtype: qss.core.job.Job/None
        """
        output = None

        if self.__latest_queued_timestamp == current_time:
            output = self.__queued_buffer_job
            self.__queued_buffer_job = None

        return output

    def get_next(self, current_time):
        """
        Get (remove and return) job from the queue.

        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        @return: Job object.
        @rtype: qss.core.job.Job
        """
        output = self.__queue.pop(0)

        self.__post_pop_job_per_source(source=output.source,
                                       current_time=current_time)
        return output

    def pull(self, eid, current_time, **kwargs):
        """
        Get (remove and return) the particular job from the queue.

        @param eid: Id of the element in the queue.
        @type eid: int
        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float

        @keyword job_id: Job id.

        @return: Job object.
        @rtype: qss.core.job.Job
        """
        output = None

        job_id, max_eid = kwargs.get('job_id'), len(self.__queue) - 1
        if job_id and (eid > max_eid or id(self.__queue[eid]) != job_id):
            for idx, job in enumerate(self.__queue):
                if id(job) == job_id:
                    output = self.__queue.pop(idx)
                    break
        elif eid <= max_eid:
            output = self.__queue.pop(eid)
        else:
            raise Exception('Defined job can not be located in the queue.')

        if output is None:
            raise Exception('Defined job is not found in the queue.')

        self.__post_pop_job_per_source(source=output.source,
                                       current_time=current_time)
        return output

    def iterator(self, limit=None):
        """
        Get the iterator over the queue.

        @param limit: The number of elements to go through.
        @type limit: int/None
        @return: Queue iterator.
        @rtype: iterator
        """
        limit = len(self.__queue) if limit is None else limit
        return islice(self.__queue, limit)
