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

from collections import defaultdict, Counter

from .constants import NodeState


class NodeManager(object):

    def __init__(self, num_nodes):
        """
        Initialization.

        @param num_nodes: Number of service nodes.
        @type num_nodes: int
        """
        self.__nodes = [NodeState.Idle for _ in range(num_nodes)]
        self.__jobs_allocation = []  # (<job>, <node_ids>)
        self.__num_jobs_per_source = defaultdict(int)

    @property
    def num_idle_nodes(self):
        """
        Get the number of idle service nodes.

        @return: Number of not busy nodes.
        @rtype: int
        """
        return Counter(self.__nodes)[NodeState.Idle]

    @property
    def num_busy_nodes(self):
        """
        Get the number of busy service nodes.

        @return: Number of busy nodes.
        @rtype: int
        """
        return Counter(self.__nodes)[NodeState.Busy]

    @property
    def num_processing_jobs(self):
        """
        Get the number of jobs to be processed.

        @return: Number of jobs allocated to service nodes.
        @rtype: int
        """
        return len(self.__jobs_allocation)

    @property
    def next_release_timestamp(self):
        """
        Get the closest (to the current time) timestamp.

        @return: Minimum (release) timestamp.
        @rtype: float
        """
        if self.__jobs_allocation:
            return self.__jobs_allocation[0][0].release_timestamp

    def get_scheduled_release_timestamps(self):
        """
        Get timestamps when each busy node will release the job.

        @return: Map of node ids with corresponding scheduled release timestamp.
        @rtype: dict
        """
        output = {}

        for job, node_ids in self.__jobs_allocation:
            release_timestamp = job.scheduled_release_timestamp
            output.update(dict(map(lambda x: (x, release_timestamp), node_ids)))

        return output

    def ready_for_processing(self, job):
        """
        Check availability of nodes to start the job processing.

        @param job: Job object.
        @type job: qss.core.job.Job
        @return: Flag that job processing can be started.
        @rtype: bool
        """
        output = True

        if self.num_idle_nodes < job.num_nodes:
            output = False

        return output

    def start_processing(self, job, current_time):
        """
        Assign job to the defined number of idle nodes for processing.

        @param job: Job object.
        @type job: qss.core.job.Job
        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        """
        if self.num_idle_nodes < job.num_nodes:
            raise Exception('The number of requested nodes exceeds ' +
                            'the number of idle nodes.')

        node_ids = []
        for node_id, node_state in enumerate(self.__nodes):
            if node_state == NodeState.Idle:
                self.__nodes[node_id] = NodeState.Busy
                node_ids.append(node_id)
                if len(node_ids) == job.num_nodes:
                    break

        job.submission_timestamp = current_time
        self.__jobs_allocation.append((job, node_ids))
        self.__jobs_allocation.sort(key=lambda x: x[0].release_timestamp)
        self.__increase_num_jobs_per_source(source=job.source)

    def stop_processing(self, current_time):
        """
        Release scheduled service nodes and get finished jobs.

        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        @return: Finished (processed) jobs.
        @rtype: list
        """
        output = []

        while current_time == self.next_release_timestamp:

            job, node_ids = self.__jobs_allocation.pop(0)
            output.append(job)
            self.__decrease_num_jobs_per_source(source=job.source)

            for node_id in node_ids:
                self.__nodes[node_id] = NodeState.Idle

        return output

    def assign_processing(self, job, node_ids, current_time):
        """
        Assign job to the defined nodes according to the provided ids.

        @param job: Job object.
        @type job: qss.core.job.Job
        @param node_ids: List of ids of the requested nodes.
        @type node_ids: list
        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        """
        if len(node_ids) != job.num_nodes:
            raise Exception('The number of requested nodes does not ' +
                            'correspond to the number of provided nodes.')

        failed_node_id = None
        for node_id in node_ids:

            if self.__nodes[node_id] == NodeState.Busy:
                failed_node_id = node_id
                break

            self.__nodes[node_id] = NodeState.Busy

        if failed_node_id:
            for node_id in node_ids[:node_ids.index(failed_node_id)]:
                self.__nodes[node_id] = NodeState.Idle
            raise Exception('Already busy (assigned) node was requested again.')

        job.submission_timestamp = current_time
        self.__jobs_allocation.append((job, node_ids))
        self.__jobs_allocation.sort(key=lambda x: x[0].release_timestamp)
        self.__increase_num_jobs_per_source(source=job.source)

    def reset(self):
        """
        Reset all service nodes (set nodes to the idle state).
        """
        if self.num_busy_nodes:
            for node_id, node_state in enumerate(self.__nodes):
                if node_state == NodeState.Busy:
                    self.__nodes[node_id] = NodeState.Idle

        del self.__jobs_allocation[:]
        self.__num_jobs_per_source.clear()

    def __increase_num_jobs_per_source(self, source):
        """
        Increase the number of jobs from the specific source.

        @param source: Source name of the job.
        @type source: str
        """
        self.__num_jobs_per_source[source] += 1

    def __decrease_num_jobs_per_source(self, source):
        """
        Decrease the number of jobs from the specific source.

        @param source: Source name of the job.
        @type source: str
        """
        if source in self.__num_jobs_per_source:
            self.__num_jobs_per_source[source] -= 1

            if not self.__num_jobs_per_source[source]:
                del self.__num_jobs_per_source[source]

    def get_num_jobs_with_source_names(self):
        """
        Get the number of jobs with corresponding source names.

        @return: Pairs of source names and the corresponding number of jobs.
        @rtype: tuple(str, int)
        """
        return self.__num_jobs_per_source.items()
