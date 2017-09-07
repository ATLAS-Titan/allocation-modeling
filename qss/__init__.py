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
# - Alexey Poyda, <poyda@wdcb.ru>, 2017
#

from .core import Queue, ServiceManager, stream_generator
from .utils import EnumTypes


ServiceState = EnumTypes(
    ('Arrival', 0),
    ('Completion', 1),
    ('Stop', 2)
)


class QSS(object):

    """Queueing System Simulator."""

    def __init__(self, num_nodes, queue_limit=None):
        """
        Initialization.

        @param num_nodes: Number of service nodes.
        @type num_nodes: int
        @param queue_limit: Maximum number of elements (jobs) in queue.
        @type queue_limit: int/None
        """
        self.__current_state = None
        self.__current_time = 0.

        self.__job_generator = None
        self.__arrival_job = None

        self.__queue = Queue(queue_limit)

        self.__output = []
        self.__service_manager = ServiceManager(num_nodes=num_nodes,
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

    def __set_next_arrival_job(self):
        """
        Define the next arrival job (by the corresponding generator).
        """
        try:
            self.__arrival_job = self.__job_generator.next()
        except StopIteration:
            self.__arrival_job = None

    def __set_next_timestamp(self):
        """
        Define the next timestamp based on the closest action that is scheduled.
        """
        next_arrival_timestamp = (0. if not self.__arrival_job
                                  else self.__arrival_job.arrival_timestamp)
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
            self.__trace_update()

        elif self.__current_state == ServiceState.Completion:
            self.__completion()
            self.__submission()
            self.__trace_update()

        elif self.__current_state == ServiceState.Stop:
            output = 1

        return output

    def __arrival(self):
        """
        Get new (generated) job and put it to the queue.
        """
        self.__queue.add(self.__arrival_job)
        self.__set_next_arrival_job()

    def __submission(self):
        """
        Get jobs from the queue and submit them to idle service nodes.
        """
        while (not self.__queue.is_empty
                and not self.__service_manager.all_nodes_busy):

            exec_code = self.__service_manager.start_job_processing(
                current_time=self.__current_time, job=self.__queue.pop())

            if exec_code:
                break

    def __completion(self):
        """
        Release service nodes if the job's processing is done.
        """
        self.__service_manager.stop_job_processing(
            current_time=self.__current_time)

    def __trace_update(self):
        """
        Update tracing data.
        """
        self.__trace.append((self.__current_time,
                             self.__queue.length,
                             self.__service_manager.num_busy_nodes))

    def __reset(self):
        """
        Reset parameters.
        """
        self.__current_state = None
        self.__current_time = 0.

        self.__arrival_job = None

        self.__queue.reset()
        self.__service_manager.reset()

        del self.__output[:]
        del self.__trace[:]

    def get_avg_num_jobs(self):
        """
        Get average number of jobs in the system.

        @return: Average number.
        @rtype: float
        """
        output = 0.

        if self.__trace:
            for i in range(0, len(self.__trace) - 1):
                num_jobs = self.__trace[i][1] + self.__trace[i][2]
                dt = self.__trace[i + 1][0] - self.__trace[i][0]
                output += num_jobs * dt

            output = output / (self.__trace[-1][0] - self.__trace[0][0])

        return output

    def get_avg_len_queue(self):
        """
        Get average length of the queue.

        @return: Average number.
        @rtype: float
        """
        output = 0.

        if self.__trace:
            for i in range(0, len(self.__trace) - 1):
                num_jobs_in_queue = self.__trace[i][1]
                dt = self.__trace[i + 1][0] - self.__trace[i][0]
                output += num_jobs_in_queue * dt

            output = output / (self.__trace[-1][0] - self.__trace[0][0])

        return output

    def get_avg_delay(self):
        """
        Get average job's delay (wait time + service time).

        @return: Average number.
        @rtype: float
        """
        output = 0.

        if self.__output:
            output = (reduce(lambda x, y: x + y.delay, self.__output, 0.)
                      / len(self.__output))

        return output

    def print_stats(self):
        """
        Print statistics.
        """
        if not self.trace and not self.output_channel:
            return

        print 'AVG number of jobs: {0}; AVG queue length: {1}'.format(
            self.get_avg_num_jobs(),
            self.get_avg_len_queue())
        print 'AVG delay: {0}'.format(self.get_avg_delay())

        if self.__queue.num_dropped:
            print 'Drop rate: {0}'.format(
                self.__queue.num_dropped /
                (self.__queue.num_dropped + len(self.output_channel)))

    def run(self, stream):
        """
        Run simulation.

        @param stream: Input stream that generates jobs.
        @type stream: generator
        """
        self.__reset()

        if not stream:
            raise Exception('Stream generator is not set.')
        else:
            self.__job_generator = stream

        self.__set_next_arrival_job()
        while True:
            status_code = self.__next_action()
            if status_code:
                break
            self.__set_next_timestamp()
