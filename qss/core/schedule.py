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
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2018
# - Alexey Poyda, <poyda@wdcb.ru>, 2018
#

from multiprocessing.dummy import Pool as ThreadPool

THREADS_NUMBER = 100
THRESHOLD_NUMBER = 1000


class NodeSchedule(object):

    def __init__(self):
        """
        Initialization.
        """
        self.__timetable = []  # (<busy_start_time>, <busy_end_time>)

    def reset(self):
        """
        Reset internal parameters.
        """
        del self.__timetable[:]

    def get_start_timestamps(self, wall_time, current_time):
        """
        Get timestamps that correspond to the periods when job can be started.

        @param wall_time: Required processing time for the job.
        @type wall_time: float
        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        @return: Tuples with timestamp and 1/-1 value (inc/dec).
        @rtype: <generator>
        """
        idle_start_timestamp = current_time
        for record in self.__timetable:

            if (idle_start_timestamp < record[0]
                    and record[0] - idle_start_timestamp >= wall_time):
                yield (idle_start_timestamp, 1)
                yield (record[0] - wall_time, -1)

            elif record[1] < idle_start_timestamp:
                continue

            idle_start_timestamp = record[1]

        yield (idle_start_timestamp, 1)

    def insert(self, start_timestamp, end_timestamp):
        """
        Add record to the schedule for the Node object.

        @param start_timestamp: Scheduled start time for the job processing.
        @type start_timestamp: float
        @param end_timestamp: Scheduled end time of the job processing.
        @type end_timestamp: float
        """
        idx, ex_record = 0, (None, None)
        while True:

            try:
                record = self.__timetable[idx]
            except IndexError:
                record = None

            if ex_record[1] < start_timestamp:
                if record is None or end_timestamp < record[0]:
                    self.__timetable.insert(
                        idx, (start_timestamp, end_timestamp))
                    break
                elif record and end_timestamp == record[0]:
                    self.__timetable[idx] = start_timestamp, record[1]
                    break
            elif ex_record[1] == start_timestamp:
                if record is None or end_timestamp < record[0]:
                    self.__timetable[idx - 1] = ex_record[0], end_timestamp
                    break
                elif record and end_timestamp == record[0]:
                    self.__timetable[idx - 1] = (
                        ex_record[0], self.__timetable.pop(idx)[1])
                    break
            elif ex_record[1] > start_timestamp:
                raise Exception('New record cannot fit to the idle period.')

            idx += 1
            ex_record = record


class ScheduleManager(object):

    def __init__(self, num_nodes):
        """
        Initialization.

        @param num_nodes: Number of service nodes.
        @type num_nodes: int
        """
        self.__current_time = 0.

        self.__schedules = [NodeSchedule() for _ in range(num_nodes)]
        self.__scheduled_start_data = []

    def __get_schedule_parameters(self, job):
        """
        Get parameters to insert the job into schedules.

        @param job: Job object.
        @type job: qss.core.job.Job
        @return: Start timestamp and list of schedule/node ids.
        @rtype: tuple
        """
        wall_time, num_nodes = job.wall_time, job.num_nodes

        if not wall_time or not num_nodes:
            raise Exception('Job\'s wall time or the number of requested ' +
                            'nodes is not defined.')
        elif num_nodes > len(self.__schedules):
            raise Exception('The number of requested nodes exceeds ' +
                            'the total number.')

        cumulative_start_timestamps = []
        for sched_id, schedule in enumerate(self.__schedules):
            cumulative_start_timestamps.insert(
                sched_id, schedule.get_start_timestamps(
                    wall_time=wall_time, current_time=self.__current_time))

        next_timestamps = []
        for sched_id, timestamps in enumerate(cumulative_start_timestamps):
            timestamp, value = timestamps.next()
            next_timestamps.append((timestamp, sched_id, value))
        next_timestamps.sort()

        start_timestamp, schedule_ids = None, []
        while True:

            timestamp, sched_id, value = next_timestamps.pop(0)
            if value > 0:
                schedule_ids.append(sched_id)
            else:
                schedule_ids.remove(sched_id)

            num_nodes -= value
            if not num_nodes:
                start_timestamp = timestamp
                break

            try:
                next_timestamp, next_value = \
                    cumulative_start_timestamps[sched_id].next()
            except StopIteration:
                pass
            else:
                next_timestamps.append((next_timestamp, sched_id, next_value))
                next_timestamps.sort()

        return start_timestamp, schedule_ids

    @property
    def next_start_timestamp(self):
        """
        Get the next [scheduled] start timestamp.

        @return: Start timestamp.
        @rtype: float
        """
        if self.__scheduled_start_data:
            return self.__scheduled_start_data[0][0]

    def is_backfill_job(self, job_id):
        """
        Check whether the job is scheduled next (backfill job).

        @param job_id: Job id.
        @type job_id: int
        @return: Flag that job will be processed next.
        @rtype: bool
        """
        output = False

        for record in self.__scheduled_start_data:
            if self.__current_time != record[0]:
                break
            elif job_id == record[1]:
                output = True
                break

        return output

    def add(self, job, current_time=None):
        """
        Add one job to the schedule.

        @param job: Job object.
        @type job: qss.core.job.Job
        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        """
        if current_time is not None:
            self.__current_time = current_time

        if job.wall_time == 0.:
            return

        start_timestamp, schedule_ids = self.__get_schedule_parameters(job=job)
        end_timestamp = start_timestamp + job.wall_time

        def insert_record(schedule_id):
            return self.__schedules[schedule_id].insert(
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp)

        if len(schedule_ids) < THRESHOLD_NUMBER:
            map(insert_record, schedule_ids)
        else:
            pool = ThreadPool(THREADS_NUMBER)
            pool.map(insert_record, schedule_ids)
            pool.close()
            pool.join()

        position_id = 0
        for record in self.__scheduled_start_data:
            if start_timestamp <= record[0]:
                break
            position_id += 1
        self.__scheduled_start_data.insert(position_id, (start_timestamp,
                                                         id(job),
                                                         schedule_ids))

    def set_initial_busy_times(self, node_release_timestamps, current_time):
        """
        Set the first busy time period for corresponding schedules.

        @param node_release_timestamps: Timestamps when busy node will be idle.
        @type node_release_timestamps: dict
        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        """
        self.__current_time = current_time

        for sched_id, schedule in enumerate(self.__schedules):

            schedule.reset()
            if sched_id in node_release_timestamps:
                schedule.insert(start_timestamp=self.__current_time,
                                end_timestamp=node_release_timestamps[sched_id])

        del self.__scheduled_start_data[:]

    def create_schedule_by_queue(self, queue_iterator):
        """
        Form the cumulative schedule for all elements of the queue.

        @param queue_iterator: Queue iterator.
        @type queue_iterator: iterator
        """
        for job in queue_iterator:
            self.add(job=job)

    def has_scheduled_elements(self, current_time):
        """
        Check that there are elements that are scheduled to be processed.

        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float/None
        @return: Flag that there is at least one job that will be processed.
        @rtype: bool
        """
        output = False

        if current_time == self.next_start_timestamp:
            output = True

        return output

    def get_scheduled_elements(self, current_time):
        """
        Get elements that are scheduled to be processed.

        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float/None
        @return: List of tuples (job_id, node_ids) of jobs to be processed.
        @rtype: list
        """
        output = []

        if current_time == self.next_start_timestamp:
            self.__current_time = current_time
            while self.__current_time == self.next_start_timestamp:
                output.append(self.__scheduled_start_data.pop(0)[1:3])

        return output
