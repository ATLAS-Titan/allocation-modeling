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
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2017
#

from collections import defaultdict

try:
    from ..policy import QUEUE_POLICY
except ImportError:
    QUEUE_POLICY = {}


class Queue(object):

    def __init__(self, policy=None, total_limit=None, with_buffer=False):
        """
        Initialization (limits are applied to the queue excluding the buffer).

        @param policy: Policy for queue behaviour.
        @type policy: dict/None
        @param total_limit: Maximum number of jobs in the queue.
        @type total_limit: int/None
        @param with_buffer: Flag to use buffer (instead of drop the job).
        @type with_buffer: bool
        """
        self.__data = []

        policy = policy or QUEUE_POLICY

        self.__limit_policy = policy.get('limit', {})
        if total_limit:
            self.__limit_policy['_total'] = total_limit

        self.__num_labeled_jobs = defaultdict(int)

        if with_buffer:
            self.__buffer_by_label = defaultdict(list)
            self.__num_dropped = None
        else:
            self.__num_dropped = defaultdict(int, {'_total': 0})
            self.__buffer_by_label = None

    @property
    def is_empty(self):
        """
        Flag shows whether queue is empty or not.

        @return: Flag that queue is empty.
        @rtype: bool
        """
        return True if not self.__data else False

    @property
    def length(self):
        """
        Get the number of all jobs in the queue.

        @return: Number of jobs.
        @rtype: int
        """
        return len(self.__data)

    @property
    def length_buffer(self):
        """
        Get the number of all jobs in the buffer.

        @return: Number of jobs.
        @rtype: int
        """
        output = 0

        if self.__buffer_by_label is not None:
            output = sum(map(lambda x: len(x), self.__buffer_by_label.values()))

        return output

    @property
    def length_total(self):
        """
        Get the number of all jobs in the queue and its buffer.

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
            if (self.__buffer_by_label is not None
                    and label in self.__buffer_by_label):
                output = len(self.__buffer_by_label[label])

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
            if self.__buffer_by_label is not None:
                output = map(lambda (k, v): (k, len(v)),
                             self.__buffer_by_label.items())

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

    def __process_rejected_jobs(self, element):
        """
        Process elements that were not added to the queue (due to the limit).

        @param element: Queue element (job).
        @type element: qss.core.job.Job
        """
        if self.__buffer_by_label is not None:
            self.__buffer_by_label[element.source_label].append(element)

        elif self.__num_dropped is not None:
            self.__num_dropped[element.source_label] += 1
            self.__num_dropped['_total'] += 1

    def reset(self):
        """
        Reset parameters.
        """
        del self.__data[:]

        for label in self.__num_labeled_jobs:
            self.__num_labeled_jobs[label] = 0

        if self.__buffer_by_label is not None:
            for label in self.__buffer_by_label:
                del self.__buffer_by_label[label][:]

        if self.__num_dropped is not None:
            for label in self.__num_dropped:
                self.__num_dropped[label] = 0

    def add(self, element):
        """
        Add element (job) to the queue.

        @param element: Queue element (job).
        @type element: qss.core.job.Job
        """
        with_limit, has_free_spots = False, True

        if '_total' in self.__limit_policy:
            if (self.__limit_policy['_total'] - self.length) < 1:
                has_free_spots = False
            with_limit = True

        if has_free_spots and element.source_label in self.__limit_policy:
            if (self.__limit_policy[element.source_label] -
                    self.get_num_labeled_jobs(label=element.source_label)) < 1:
                has_free_spots = False
            with_limit = True

        if not with_limit or has_free_spots:
            self.__data.append(element)
            self.__increase_num_labeled_jobs(label=element.source_label)
        elif with_limit:
            self.__process_rejected_jobs(element=element)

    def show_next(self):
        """
        Show the next element (job) without removing it from the queue.

        @return: Queue element (job).
        @rtype: qss.core.job.Job
        """
        return self.__data[0]

    def pop(self):
        """
        Get (remove and return) element (job) from the queue.

        @return: Queue element (job).
        @rtype: qss.core.job.Job
        """
        job_source_label = self.show_next().source_label

        output = self.__data.pop(0)
        self.__decrease_num_labeled_jobs(label=job_source_label)

        # get the job (of the defined label) from the buffer
        if self.get_num_labeled_jobs(label=job_source_label, in_buffer=True):
            self.add(element=self.__buffer_by_label[job_source_label].pop(0))

        return output
