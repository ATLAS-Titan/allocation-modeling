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


def random_generator(rate, number=None):
    """
    Yield randomly generated values.

    @param rate: Rate value (e.g., arrival rate).
    @type rate: float
    @param number: Number of generated elements (None -> infinite elements).
    @type: number: int/None
    @return: Random value.
    @rtype: float
    """
    _flag = not bool(number)
    while _flag or number:
        yield (-1./rate) * math.log(1. - random.random())
        if number:
            number -= 1


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

        self.__arrival_generator = None
        self.__arrival_timestamp = 0.

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

    def __next_arrival_timestamp(self):
        """
        Define the next timestamp when the new job arrives.
        """
        try:
            self.__arrival_timestamp += self.__arrival_generator.next()
        except StopIteration:
            self.__arrival_timestamp = 0.

    def __next_timestamp(self):
        """
        Define the next timestamp based on the closest action that is scheduled.
        """
        next_release_timestamp = self.__service_manager.next_release_timestamp

        if not self.__arrival_timestamp and not next_release_timestamp:
            self.__current_state = ServiceState.Stop

        elif (not next_release_timestamp or
                next_release_timestamp > self.__arrival_timestamp > 0.):
            self.__current_time = self.__arrival_timestamp
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
        self.__queue.add(JobSpecs(arrival_timestamp=self.__arrival_timestamp))
        self.__next_arrival_timestamp()

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

    def __reset(self):
        """
        Reset parameters.
        """
        self.__current_state = None
        self.__current_time = 0.
        self.__arrival_timestamp = 0.

        self.__queue.reset()
        self.__service_manager.reset()

        del self.__output[:]
        del self.__trace[:]

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

    def run(self, arrival_rate=None, arrival_generator=None,
            num_jobs=None, time_limit=None):
        """
        Run simulation.

        @param arrival_rate: Arrival rate for jobs.
        @type arrival_rate: float/None
        @param arrival_generator: Generator for random arrival times.
        @type arrival_generator: generator/None
        @param num_jobs: Number of arrival jobs.
        @type num_jobs: int/None
        @param time_limit: The maximum timestamp (until simulation is done).
        @type time_limit: float/None
        """
        self.__reset()

        if not arrival_rate and not arrival_generator:
            raise Exception('Arrival parameters are not set.')
        elif arrival_generator:
            self.__arrival_generator = arrival_generator
        else:
            self.__arrival_generator = random_generator(rate=arrival_rate,
                                                        number=num_jobs)

        if not num_jobs and not time_limit:
            raise Exception('Limits are not set.')

        self.__next_arrival_timestamp()
        while num_jobs or (time_limit and self.__current_time < time_limit):
            status_code = self.__next_action()
            if status_code:
                break
            self.__next_timestamp()
