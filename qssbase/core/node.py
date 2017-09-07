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

import math
import random


def get_random(rate):
    return (-1./rate) * math.log(1. - random.random())


class ServiceNode(object):

    """Class ServiceNode represents processing node."""

    def __init__(self, service_rate, output_channel):
        """
        Initialization.

        @param service_rate: Processing rate of the service node.
        @type service_rate: float
        @param output_channel: List of processed jobs.
        @type output_channel: list
        """
        self._service_rate = service_rate
        self._output_channel = output_channel

        self.__job = None
        self.__locked = False

    @property
    def _service_time(self):
        """
        Generate random processing time based on processing rate.

        @return: Processing time.
        @rtype: float
        """
        return get_random(rate=self._service_rate)

    @property
    def release_timestamp(self):
        """
        Timestamp when the job will be released.

        @return: Job released timestamp.
        @rtype: float
        """
        if self.__job is not None:
            return self.__job.release_timestamp

    def reset(self):
        """
        Reset parameters.
        """
        self.__job = None
        self.__locked = False

    def lock(self, current_time, job):
        """
        Lock the service node for job processing.

        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        @param job: Job object.
        @type job: qss.core.job.JobSpecs
        """
        self.__job = job
        self.__job.submission_timestamp = current_time
        self.__job.release_timestamp = current_time + self._service_time
        self.__locked = True

    def unlock(self):
        """
        Unlock the service node after job processing is done.
        """
        self._output_channel.append(self.__job)

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

    def __init__(self, service_rate, num_nodes, output_channel):
        """
        Initialization.

        @param service_rate: Processing rate of the service node.
        @type service_rate: float
        @param num_nodes: Number of service nodes.
        @type num_nodes: int
        @param output_channel: List of processed job.
        @type output_channel: list
        """
        self.__nodes = [ServiceNode(service_rate=service_rate,
                                    output_channel=output_channel)
                        for _ in range(num_nodes)]

    @property
    def next_release_timestamp(self):
        """
        Get the closest (to the current time) timestamp.

        @return: Minimum timestamp.
        @rtype: float
        """
        output = 0.

        for node in self.__nodes:
            if (node.is_locked and node.release_timestamp and
                    (node.release_timestamp < output or not output)):
                output = node.release_timestamp

        return output

    def run_service_node(self, current_time, job):
        """
        Get the idle service node and assign the job to it.

        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        @param job: Job object.
        @type job: qss.core.job.JobSpecs
        """
        for node in self.__nodes:
            if not node.is_locked:
                node.lock(current_time=current_time, job=job)
                break

    def release_service_node(self, current_time):
        """
        Release service node that is scheduled for the current timestamp.

        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        """
        for node in self.__nodes:
            if node.release_timestamp == current_time:
                node.unlock()

    def reset(self):
        """
        Reset service nodes.
        """
        for node in self.__nodes:
            node.reset()

    @property
    def all_locked(self):
        """
        Check if all service nodes are locked.

        @return: Flag that all nodes are locked.
        @rtype: bool
        """
        output = True

        for node in self.__nodes:
            if not node.is_locked:
                output = False
                break

        return output

    @property
    def all_unlocked(self):
        """
        Check if there is at least one service node that is locked.

        @return: Flag that all nodes are unlocked.
        @rtype: bool
        """
        output = True

        for node in self.__nodes:
            if node.is_locked:
                output = False
                break

        return output

    @property
    def num_locked_nodes(self):
        """
        Get the number of working service nodes (locked nodes).

        @return: Number of locked nodes.
        @rtype: int
        """
        return len(filter(lambda x: x.is_locked, self.__nodes))
