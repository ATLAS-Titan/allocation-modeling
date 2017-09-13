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

from .constants import ActionCode, ServiceState
from .core import Queue, ServiceManager, stream_generator


class QSS(object):

    """Queueing System Simulator."""

    def __init__(self, num_nodes, queue_limit=None, output_file=None):
        """
        Initialization.

        @param num_nodes: Number of service nodes.
        @type num_nodes: int
        @param queue_limit: Maximum number of elements (jobs) in queue.
        @type queue_limit: int/None
        @param output_file: Name of file for output (addon for output_channel).
        @type output_file: str/None
        """
        self.__current_state = None
        self.__current_time = 0.

        self.__job_generators = []
        self.__job_buffer = []

        self.__queue = Queue(queue_limit)

        self.__output = []
        self.__service_manager = ServiceManager(num_nodes=num_nodes,
                                                output_channel=self.__output)

        self.__output_file = output_file
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

    def __set_next_arrival_job(self, gid):
        """
        Define the next arrival job (by the corresponding generator).

        @param gid: Generator id.
        @type gid: int
        """
        next_job_id = len(self.__job_buffer)
        if gid < next_job_id:
            pass
        elif gid == next_job_id and gid < len(self.__job_generators):
            self.__job_buffer.append(None)
        else:
            raise Exception('Generator id is out of limit.')

        try:
            self.__job_buffer[gid] = self.__job_generators[gid].next()
        except StopIteration:
            self.__job_buffer[gid] = None

    def __next_arrival_params(self):
        """
        Get parameters of the next arriving job (gid, arrival_timestamp).

        @return: Generator id with corresponding minimum (arrival) timestamp.
        @rtype: tuple
        """
        sorted_params = sorted([(i, j.arrival_timestamp)
                                for i, j in enumerate(self.__job_buffer) if j],
                               key=lambda x: x[1])

        output = (None, 0.) if not sorted_params else sorted_params[0]
        return output

    def __next_arrival_timestamp(self):
        """
        Get the closest (to the current time) timestamp.

        @return: Minimum (arrival) timestamp.
        @rtype: float
        """
        return self.__next_arrival_params()[1]

    def __set_next_timestamp(self):
        """
        Define the next timestamp based on the closest action that is scheduled.
        """
        next_arrival_timestamp = self.__next_arrival_timestamp()
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
        Get new (generated) job and put it to the queue.
        """
        gid = self.__next_arrival_params()[0]
        self.__queue.add(self.__job_buffer[gid])
        self.__set_next_arrival_job(gid=gid)

        self.__trace_update(action_code=ActionCode.Arrival)

    def __submission(self):
        """
        Get jobs from the queue and submit them to idle service nodes.
        """
        had_submission = False
        while (not self.__queue.is_empty
                and not self.__service_manager.all_nodes_busy):

            exec_code = self.__service_manager.start_job_processing(
                current_time=self.__current_time, job=self.__queue.show_next())

            if exec_code:
                break

            self.__queue.pop()
            if not had_submission:
                had_submission = True

        if had_submission:
            self.__trace_update(action_code=ActionCode.Submission)

    def __completion(self):
        """
        Release service nodes if the job's processing is done.
        """
        self.__service_manager.stop_job_processing(
            current_time=self.__current_time)

        if self.__output_file and self.__output:
            job = self.__output[-1]
            with open(self.__output_file, 'a') as f:
                f.write(','.join([
                    str(job.arrival_timestamp),
                    str(job.submission_timestamp),
                    str(job.release_timestamp),
                    str(job.num_nodes),
                    job.source_label
                ]) + '\n')

        self.__trace_update(action_code=ActionCode.Completion)

    def __trace_update(self, action_code=None):
        """
        Update tracing data.

        @param action_code: Code of the action.
        @type action_code: str/None
        """
        self.__trace.append((self.__current_time,
                             self.__queue.length,
                             self.__service_manager.num_processing_jobs,
                             action_code or '-'))

    def __reset(self):
        """
        Reset parameters.
        """
        self.__current_state = None
        self.__current_time = 0.

        del self.__job_buffer[:]

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

    def get_avg_delay(self, source_label=None):
        """
        Get average job's delay (wait time + service time).

        @param source_label: Source label (make calculations by stream)
        @type source_label: str/None
        @return: Average number.
        @rtype: float
        """
        output = 0.

        if self.__output:

            jobs = self.__output if not source_label else filter(
                lambda x: x.source_label == source_label, self.__output)

            output = reduce(lambda x, y: x + y.delay, jobs, 0.) / len(jobs)

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

    def run(self, streams, output_file=None):
        """
        Run simulation.

        @param streams: Input streams that generate jobs.
        @type streams: list of generators
        @param output_file: Name of file for output per run.
        @type output_file: str/None
        """
        if not streams:
            raise Exception('Stream generators are not set.')

        if output_file:
            self.__output_file = output_file

        self.__reset()

        self.__job_generators = streams
        for gid in range(len(self.__job_generators)):
            self.__set_next_arrival_job(gid=gid)

        while True:
            status_code = self.__next_action()
            if status_code:
                break
            self.__set_next_timestamp()
