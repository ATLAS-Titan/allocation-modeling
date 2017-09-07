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


class Node(object):

    """Class Node represents processing node."""

    def __init__(self):
        """
        Initialization.
        """
        self.__job = None
        self.__locked = False

    @property
    def submission_timestamp(self):
        """
        Timestamp when the job was submitted.

        @return: Job submission timestamp.
        @rtype: float
        """
        if self.__job is not None:
            return self.__job.submission_timestamp

    @property
    def release_timestamp(self):
        """
        Timestamp when the job will be released.

        @return: Job released timestamp.
        @rtype: float
        """
        if self.__job is not None:
            return self.__job.release_timestamp

    def get_job(self):
        """
        Get job object.

        @return: Job object.
        @rtype: qss.core.job.Job
        """
        if self.is_locked:
            return self.__job

    def lock(self, job):
        """
        Lock the service node for job processing.

        @param job: Job object.
        @type job: qss.core.job.Job
        """
        self.__job = job
        self.__locked = True

    def unlock(self):
        """
        Unlock the service node after job processing is done.
        """
        self.__job = None
        self.__locked = False

    @property
    def is_locked(self):
        """
        Flag showed whether service node is locked or not.

        @return: Flag of the service node state.
        @rtype: bool
        """
        return self.__locked


class ServiceManager(object):

    """Class ServiceManager is responsible to handle a group of nodes."""

    def __init__(self, num_nodes, output_channel):
        """
        Initialization.

        @param num_nodes: Number of service nodes.
        @type num_nodes: int
        @param output_channel: List of processed jobs.
        @type output_channel: list
        """
        self.__idle_nodes = [Node() for _ in range(num_nodes)]
        self.__busy_node_groups = []

        self.__output_channel = output_channel

    @property
    def next_release_timestamp(self):
        """
        Get the closest (to the current time) timestamp.

        @return: Minimum (release) timestamp.
        @rtype: float
        """
        if self.__busy_node_groups:
            return self.__busy_node_groups[0][0].release_timestamp

    def start_job_processing(self, current_time, job):
        """
        Assign job to the defined number of idle service nodes for processing.

        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        @param job: Job object.
        @type job: qss.core.job.Job
        @return: Operation status code (0 - success, >0 - failure).
        @rtype: int
        """
        if self.num_idle_nodes < job.num_nodes:
            return 1

        job.submission_timestamp = current_time

        nodes = self.__idle_nodes[:job.num_nodes]
        del self.__idle_nodes[:job.num_nodes]

        for node in nodes:
            node.lock(job=job)
        self.__busy_node_groups.append(nodes)

        self.__busy_node_groups.sort(key=lambda x: x[0].release_timestamp)
        return 0

    def stop_job_processing(self, current_time):
        """
        Release scheduled service nodes.

        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        """
        while (self.__busy_node_groups and
               current_time == self.__busy_node_groups[0][0].release_timestamp):

            nodes = self.__busy_node_groups.pop(0)
            self.__output_channel.append(nodes[0].get_job())

            for node in nodes:
                node.unlock()
            self.__idle_nodes.extend(nodes)

    def reset(self):
        """
        Reset (unlock) all service nodes.
        """
        while self.__busy_node_groups:
            nodes = self.__busy_node_groups.pop(0)

            for node in nodes:
                node.unlock()
            self.__idle_nodes.extend(nodes)

        if not self.all_nodes_idle:
            for node in self.__idle_nodes:
                node.unlock()

    @property
    def all_nodes_busy(self):
        """
        Check if all service nodes are locked (busy).

        @return: Flag that all nodes are busy/locked.
        @rtype: bool
        """
        return bool(self.num_busy_nodes) and not bool(self.num_idle_nodes)

    @property
    def all_nodes_idle(self):
        """
        Check if there is at least one service node that is locked (busy).

        @return: Flag that all nodes are idle/unlocked.
        @rtype: bool
        """
        return not bool(self.num_busy_nodes) and bool(self.num_idle_nodes)

    @property
    def num_busy_nodes(self):
        """
        Get the number of working service nodes (locked nodes).

        @return: Number of locked nodes.
        @rtype: int
        """
        return sum(map(lambda x: len(x), self.__busy_node_groups))

    @property
    def num_idle_nodes(self):
        """
        Get the number of idle service nodes (unlocked nodes).

        @return: Number of unlocked nodes.
        @rtype: int
        """
        return len(self.__idle_nodes)
