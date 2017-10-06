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
    from ..policies import QUEUE_POLICY
except ImportError:
    QUEUE_POLICY = {}


class Queue(object):

    def __init__(self, policy=None, total_limit=None):
        """
        Initialization.

        @param policy: Policy for queue behaviour.
        @type policy: dict/None
        @param total_limit: Maximum number of jobs in the queue.
        @type total_limit: int/None
        """
        self.__data = []

        policy = policy or QUEUE_POLICY

        self.__limit_policy = policy.get('limit', {})
        if total_limit:
            self.__limit_policy['_total'] = total_limit

        self.__num_labeled_jobs = defaultdict(int)
        self.__num_dropped = defaultdict(int, {'_total': 0})

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

    def get_num_jobs_with_labels(self):
        """
        Get the number of jobs with corresponding labels.

        @return: Pairs of labels and corresponding number of jobs.
        @rtype: tuple(str, int)
        """
        return self.__num_labeled_jobs.items()

    def get_num_labeled_jobs(self, label):
        """
        Get the number of jobs in the queue by label.

        @param label: Source label of the job.
        @type label: str
        @return: Number of jobs.
        @rtype: int
        """
        output = self.__num_labeled_jobs.get(label)

        if output is None:
            output = len(filter(lambda x: x.source_label == label, self.__data))

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
        return self.__num_dropped['_total']

    def get_num_dropped_with_labels(self):
        """
        Get the number of dropped jobs with corresponding labels.

        @return: Pairs of labels and corresponding number of dropped jobs.
        @rtype: tuple(str, int)
        """
        return self.__num_dropped.items()

    def get_labeled_num_dropped(self, label):
        """
        Get the number of dropped jobs by label.

        @param label: Source label of the job.
        @type label: str
        @return: Number of dropped jobs.
        @rtype: int
        """
        return self.__num_dropped.get(label, 0)

    def __increase_num_dropped(self, label):
        """
        Increase the number of dropped jobs (in the queue).

        @param label: Source label of the job.
        @type label: str
        """
        self.__num_dropped[label] += 1
        self.__num_dropped['_total'] += 1

    def reset(self):
        """
        Reset parameters.
        """
        del self.__data[:]

        for label in self.__num_labeled_jobs:
            self.__num_labeled_jobs[label] = 0

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
            self.__increase_num_dropped(label=element.source_label)

    def show_next(self):
        """
        Show next element (job) without removing it from the queue.

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
        self.__decrease_num_labeled_jobs(label=self.show_next().source_label)
        return self.__data.pop(0)
