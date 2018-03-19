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

    """Class QueueManager is responsible to control and manage the queue."""

    def __init__(self, policy=None, limit=None, with_buffer=False):
        """
        Initialization (limits are applied to the queue excluding the buffer).

        @param policy: Policy for queue behaviour.
        @type policy: dict/None
        @param limit: Maximum (total) number of jobs in the queue.
        @type limit: int/None
        @param with_buffer: Flag to use buffer (instead of drop the job).
        @type with_buffer: bool
        """
        self.__queue = []
        self.__latest_queued_timestamp = 0.

        self.__num_labeled_jobs = defaultdict(int)

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

    def get_num_labeled_jobs(self, label, in_buffer=False):
        """
        Get the number of jobs in the queue/buffer by label.

        @param label: Source label of the job.
        @type label: str
        @param in_buffer: Flag to count jobs in the buffer.
        @type in_buffer: bool
        @return: Number of jobs.
        @rtype: int
        """
        if not in_buffer:
            output = self.__num_labeled_jobs[label]
        else:
            output = 0
            if self.__buffer is not None and label in self.__buffer:
                output = len(self.__buffer[label])

        return output

    def get_num_jobs_with_labels(self, in_buffer=False):
        """
        Get the number of jobs with corresponding labels.

        @param in_buffer: Flag to count jobs in the buffer.
        @type in_buffer: bool
        @return: Pairs of labels and corresponding number of jobs.
        @rtype: list((str, int))
        """
        if not in_buffer:
            output = self.__num_labeled_jobs.items()
        else:
            output = []
            if self.__buffer is not None:
                output = map(lambda (k, v): (k, len(v)), self.__buffer.items())

        return output

    def __increase_num_labeled_jobs(self, label):
        """
        Increase the number of labeled jobs (in the queue).

        @param label: Source label of the job.
        @type label: str
        """
        self.__num_labeled_jobs[label] += 1

    def __decrease_num_labeled_jobs(self, label):
        """
        Decrease the number of labeled jobs (in the queue).

        @param label: Source label of the job.
        @type label: str
        """
        if label in self.__num_labeled_jobs:
            self.__num_labeled_jobs[label] -= 1

    @property
    def num_dropped(self):
        """
        Get the number of all dropped jobs.

        @return: Number of dropped jobs.
        @rtype: int
        """
        return 0 if self.__num_dropped is None else self.__num_dropped['_total']

    def get_labeled_num_dropped(self, label):
        """
        Get the number of dropped jobs by label.

        @param label: Source label of the job.
        @type label: str
        @return: Number of dropped jobs.
        @rtype: int
        """
        return 0 if self.__num_dropped is None else self.__num_dropped[label]

    def get_num_dropped_with_labels(self):
        """
        Get the number of dropped jobs with corresponding labels.

        @return: Pairs of labels and corresponding number of dropped jobs.
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
            for element in self.__queue:
                element.increase_priority(value=time_delta)
            self.__latest_queued_timestamp = current_time

        self.__job_init(job=job)
        self.__queue_append(queue=self.__queue, element=job)
        self.__increase_num_labeled_jobs(label=job.source_label)

    def __process_rejected_job(self, job):
        """
        Process element that was not added to the queue (due to the limit).

        @param job: Job object.
        @type job: qss.core.job.Job
        """
        if self.__buffer is not None:
            self.__buffer[job.source_label].append(job)

        elif self.__num_dropped is not None:
            self.__num_dropped[job.source_label] += 1
            self.__num_dropped['_total'] += 1

    def reset(self):
        """
        Reset parameters.
        """
        del self.__queue[:]
        self.__latest_queued_timestamp = 0.

        for label in self.__num_labeled_jobs:
            self.__num_labeled_jobs[label] = 0

        if self.__buffer is not None:
            for label in self.__buffer:
                del self.__buffer[label][:]

        if self.__num_dropped is not None:
            for label in self.__num_dropped:
                self.__num_dropped[label] = 0

    def add(self, job, current_time):
        """
        Add element (job) to the queue.

        @param job: Job object.
        @type job: qss.core.job.Job
        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        """
        with_limit, has_free_spots = False, True

        if '_total' in self.__limits:
            if (self.__limits['_total'] - self.length) < 1:
                has_free_spots = False
            with_limit = True

        if has_free_spots and job.source_label in self.__limits:
            if (self.__limits[job.source_label] -
                    self.get_num_labeled_jobs(label=job.source_label)) < 1:
                has_free_spots = False
            with_limit = True

        if not with_limit or has_free_spots:
            self.__process_approved_job(job=job, current_time=current_time)
        elif with_limit:
            self.__process_rejected_job(job=job)

    def show_next(self):
        """
        Show the next available job without removing it from the queue.

        @return: Job object.
        @rtype: qss.core.job.Job
        """
        return self.__queue[0]

    def get_next(self, current_time):
        """
        Get (remove and return) job from the queue.

        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        @return: Job object.
        @rtype: qss.core.job.Job
        """
        output = self.__queue.pop(0)
        self.__decrease_num_labeled_jobs(label=output.source_label)

        # get the job (of the defined label) from the buffer
        if self.get_num_labeled_jobs(label=output.source_label, in_buffer=True):
            self.add(job=self.__buffer[output.source_label].pop(0),
                     current_time=current_time)

        return output
