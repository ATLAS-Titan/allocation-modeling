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

from .core import JobSpecs, ServiceManager, ServiceQueue
from .utils import EnumTypes


ServiceState = EnumTypes(
    ('Arrival', 0),
    ('Completion', 1),
    ('Stop', 2)
)


def random_generator(rate):
    return (-1/rate) * math.log(1 - random.random())


class QSS(object):

    """Queueing System Simulator."""

    def __init__(self, service_rate, num_nodes, queue_limit=None):
        """
        Initialization.

        @param service_rate: Processing rate of the service node.
        @type service_rate: float
        @param num_nodes: Number of service nodes.
        @type num_nodes: int
        @param queue_limit: Maximum number of elements (jobs) in queue.
        @type queue_limit: int/None
        """
        self.__current_state = None
        self.__current_time = 0.

        self.__arrival_timestamps = []
        self.__queue = ServiceQueue(queue_limit)

        self.__output = []
        self.__service_manager = ServiceManager(service_rate=service_rate,
                                                num_nodes=num_nodes,
                                                output_channel=self.__output)

        self.__trace = []

    @property
    def output_channel(self):
        """
        Get list of processed jobs.

        @return: Processed jobs.
        @rtype: list
        """
        return self.__output

    @property
    def trace(self):
        """
        Get trace data.

        @return: Trace data.
        @rtype: list
        """
        return self.__trace

    @property
    def next_arrival_time(self):
        """
        Get the next timestamp when the new job arrives.

        @return: Job arrival timestamp.
        @rtype: float
        """
        output = 0.

        if self.__arrival_timestamps:
            output = self.__arrival_timestamps[0]

        return output

    def __next_timestamp(self):
        """
        Set the next timestamp based on the closest action that is scheduled.
        """
        next_arrival_timestamp = self.next_arrival_time
        next_release_timestamp = self.__service_manager.next_release_timestamp

        if not next_arrival_timestamp and not next_release_timestamp:
            self.__current_state = ServiceState.Stop

        elif (not next_release_timestamp or
                next_release_timestamp > next_arrival_timestamp > 0.):
            self.__current_time = next_arrival_timestamp
            self.__current_state = ServiceState.Arrival

        elif next_release_timestamp:
            self.__current_time = next_release_timestamp
            self.__current_state = ServiceState.Completion

    def __next_action(self):
        """
        Run corresponding method based on the current system state.

        @return: Workflow status code.
        @rtype: int
        """
        output = 0

        if self.__current_state == ServiceState.Arrival:
            self.__arrival()
            self.__submission()
        elif self.__current_state == ServiceState.Completion:
            self.__completion()
            self.__submission()
        elif self.__current_state == ServiceState.Stop:
            output = 1

        return output

    def __arrival(self):
        """
        Get new jobs (based on scheduled arrival time) and put to the queue.
        """
        self.__queue.add(
            JobSpecs(arrival_timestamp=self.__arrival_timestamps.pop(0)))

        # track the queue if only all nodes are busy
        if self.__service_manager.all_locked:
            self.__trace_update()

    def __submission(self):
        """
        Get jobs from the queue and submit to the service node.
        """
        _new_submissions = False
        while (not self.__queue.is_empty
                and not self.__service_manager.all_locked):

            self.__service_manager.run_service_node(
                current_time=self.__current_time, job=self.__queue.pop())

            if not _new_submissions:
                _new_submissions = True

        # track the actual submission only
        if _new_submissions:
            self.__trace_update()

    def __completion(self):
        """
        Release the service node if the job's processing is done.
        """
        self.__service_manager.release_service_node(
            current_time=self.__current_time)

        # track the completion process if only the queue is empty
        if self.__queue.is_empty:
            self.__trace_update()

    def __trace_update(self):
        """
        Update tracing data.
        """
        self.__trace.append((self.__current_time,
                             self.__queue.length,
                             self.__service_manager.num_locked_nodes))

    def print_stats(self):
        """
        Print statistics.
        """
        if not self.output_channel or not self.trace:
            return

        print 'AVG delay: {0}'.format(
            reduce(lambda x, y: x + y.delay, self.output_channel, 0.)
            / len(self.output_channel))

        print 'AVG number of jobs: {0}; AVG queue length: {1}'.format(
            reduce(lambda x, y: x + y, map(lambda x: (x[1] + x[2]), self.trace))
            / float(len(self.trace)),
            reduce(lambda x, y: x + y, map(lambda x: x[1], self.trace))
            / float(len(self.trace)))

        if self.__queue.num_dropped:
            print 'Drop rate: {0}'.format(
                self.__queue.num_dropped / len(self.output_channel))

    def run(self, arrival_times, time_limit):
        """
        Run simulation.

        @param arrival_times: List of jobs' arrival times (random values).
        @type arrival_times: list
        @param time_limit: The maximum timestamp (until simulation is done).
        @type time_limit: float
        """
        if not arrival_times:
            raise Exception("Sequence of jobs' times is not set.")

        # create arrival timeline (timestamps when new job arrives)
        _current_timestamp = 0.
        for arrival_time in arrival_times:
            _current_timestamp += arrival_time
            self.__arrival_timestamps.append(_current_timestamp)

        while self.__current_time < time_limit:
            status_code = self.__next_action()
            if status_code:
                break
            self.__next_timestamp()
