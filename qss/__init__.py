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

from .stream import stream_generator, stream_generator_by_file

from .core import QueueManager, NodeManager
from .core.constants import ActionCode, ServiceState

try:
    from .policy import QUEUE_POLICY
except ImportError:
    QUEUE_POLICY = {}


class QSS(object):

    """Queueing System Simulator."""

    def __init__(self, num_nodes, queue_limit=None, use_queue_buffer=False,
                 time_limit=None, output_file=None, trace_file=None):
        """
        Initialization.

        @param num_nodes: Number of service nodes.
        @type num_nodes: int
        @param queue_limit: Maximum number of elements (jobs) in queue.
        @type queue_limit: int/None
        @param use_queue_buffer: Flag to use queue buffer (if it's necessary).
        @type use_queue_buffer: bool
        @param time_limit: The maximum timestamp (until simulator is done).
        @type time_limit: float/None
        @param output_file: Name of file for output (addon for output_channel).
        @type output_file: str/None
        @param trace_file: Name of file for trace info (addon for trace).
        @type trace_file: str/None
        """
        self.__current_state = None
        self.__current_time = 0.
        self.__time_limit = time_limit

        self.__job_generators = []
        self.__input_jobs = []

        self.__queue = QueueManager(policy=QUEUE_POLICY,
                                    limit=queue_limit,
                                    with_buffer=use_queue_buffer)

        self.__node_manager = NodeManager(num_nodes=num_nodes)

        self.__output = []
        self.__trace = []

        self.__output_file = output_file
        self.__trace_file = trace_file

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
        next_gid = len(self.__input_jobs)
        if gid < next_gid:
            pass
        elif gid == next_gid and gid < len(self.__job_generators):
            self.__input_jobs.append(None)
        else:
            raise Exception('Generator id is out of limit.')

        try:
            self.__input_jobs[gid] = self.__job_generators[gid].next()
        except StopIteration:
            self.__input_jobs[gid] = None

    def __next_arrival_params(self):
        """
        Get parameters of the next arriving job (gid, arrival_timestamp).

        @return: Generator id with corresponding minimum (arrival) timestamp.
        @rtype: tuple
        """
        sorted_params = sorted([(i, j.arrival_timestamp)
                                for i, j in enumerate(self.__input_jobs) if j],
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
        next_release_timestamp = self.__node_manager.next_release_timestamp

        if not next_arrival_timestamp and not next_release_timestamp:
            self.__current_state = ServiceState.Stop

        else:

            if (not next_release_timestamp or
                    next_release_timestamp > next_arrival_timestamp > 0.):
                self.__current_time = next_arrival_timestamp
                self.__current_state = ServiceState.Arrival
            elif next_release_timestamp:
                self.__current_time = next_release_timestamp
                self.__current_state = ServiceState.Completion

            if (self.__time_limit and self.__current_time and
                    self.__time_limit < self.__current_time):
                self.__current_state = ServiceState.Stop

    def __next_action(self, verbose=False):
        """
        Run corresponding method based on the current system state.

        @param verbose: Flag to get (show) logs.
        @type verbose: bool
        @return: Workflow status code.
        @rtype: int
        """
        output = 0

        if self.__current_state == ServiceState.Arrival:
            self.__arrival(verbose=verbose)
            self.__submission(verbose=verbose)

        elif self.__current_state == ServiceState.Completion:
            self.__completion(verbose=verbose)
            self.__submission(verbose=verbose)

        elif self.__current_state == ServiceState.Stop:
            output = 1

        return output

    def __arrival(self, verbose=False):
        """
        Get new (generated) job and put it to the queue.

        @param verbose: Flag to get (show) logs.
        @type verbose: bool
        """
        gid = self.__next_arrival_params()[0]
        self.__queue.add(job=self.__input_jobs[gid],
                         current_time=self.__current_time)
        self.__set_next_arrival_job(gid=gid)

        self.__trace_update(verbose=verbose,
                            action_code=ActionCode.Arrival)

    def __submission(self, verbose=False):
        """
        Get jobs from the queue and submit them to idle service nodes.

        @param verbose: Flag to get (show) logs.
        @type verbose: bool
        """
        had_submission = False
        while (not self.__queue.is_empty
                and not self.__node_manager.all_nodes_busy):

            if not self.__node_manager.ready_for_processing(
                    job=self.__queue.show_next()):
                break

            self.__node_manager.start_processing(
                current_time=self.__current_time,
                job=self.__queue.get_next(current_time=self.__current_time))

            if not had_submission:
                had_submission = True

        if had_submission:
            self.__trace_update(verbose=verbose,
                                action_code=ActionCode.Submission)

    def __completion(self, verbose=False):
        """
        Release service nodes if the job's processing is done.

        @param verbose: Flag to get (show) logs.
        @type verbose: bool
        """
        self.__output.extend(self.__node_manager.stop_processing(
            current_time=self.__current_time))

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

        self.__trace_update(verbose=verbose,
                            action_code=ActionCode.Completion)

    def __trace_update(self, verbose=False, action_code=None):
        """
        Update tracing data.

        @param action_code: Code of the action.
        @type action_code: str/None
        """
        self.__trace.append((self.__current_time,
                             self.__queue.length,
                             self.__node_manager.num_processing_jobs,
                             action_code or '-'))

        if verbose or self.__trace_file:

            detailed_trace_string = '{0:15f} - {1} - {2} - {3} - {4}'.format(
                self.__current_time,
                self.__queue.get_num_jobs_with_labels(in_buffer=True),
                self.__queue.get_num_jobs_with_labels(),
                self.__node_manager.get_num_jobs_with_labels(),
                self.__trace[-1][3])

            if verbose:
                print detailed_trace_string

            if self.__trace_file:
                with open(self.__trace_file, 'a') as f:
                    f.write(detailed_trace_string + '\n')

    def __reset(self):
        """
        Reset parameters.
        """
        self.__current_state = None
        self.__current_time = 0.

        del self.__input_jobs[:]

        self.__queue.reset()
        self.__node_manager.reset()

        del self.__output[:]
        del self.__trace[:]

    def get_avg_num_jobs(self):
        """
        Get average number of jobs in the system.

        @return: Average number.
        @rtype: float
        """
        output = 0.

        if self.__trace and (self.__trace[-1][0] - self.__trace[0][0]):
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

        if self.__trace and (self.__trace[-1][0] - self.__trace[0][0]):
            for i in range(0, len(self.__trace) - 1):
                num_jobs_in_queue = self.__trace[i][1]
                dt = self.__trace[i + 1][0] - self.__trace[i][0]
                output += num_jobs_in_queue * dt

            output = output / (self.__trace[-1][0] - self.__trace[0][0])

        return output

    def get_avg_delay(self, source_label=None):
        """
        Get average job's delay (wait time + service time).

        @param source_label: Source label (make calculations by stream).
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

    def get_utilization_value(self, source_label=None):
        """
        Get the utilization value.

        @param source_label: Source label (make calculations by stream).
        @type source_label: str/None
        @return: Utilization value.
        @rtype: float
        """
        output = 0.

        if self.__output:
            jobs = self.__output if not source_label else filter(
                lambda x: x.source_label == source_label, self.__output)

            output = reduce(
                lambda x, y: x + (y.num_nodes * y.execution_time), jobs, 0.)

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
            print 'Queue drop rate: {0}'.format(
                float(self.__queue.num_dropped) /
                (self.__queue.num_dropped + len(self.output_channel)))
            drop_pairs = self.__queue.get_num_dropped_with_labels()
            if len(drop_pairs) > 1:
                print 'Dropped jobs in queue (by label): {0}'.format(drop_pairs)

    def run(self, streams, verbose=False, output_file=None):
        """
        Run simulation.

        @param streams: Input streams that generate jobs.
        @type streams: list of generators
        @param verbose: Flag to get (show) logs.
        @type verbose: bool
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
            status_code = self.__next_action(verbose=verbose)
            if status_code:
                break
            self.__set_next_timestamp()
