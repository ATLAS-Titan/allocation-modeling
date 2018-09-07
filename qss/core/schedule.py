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

    def get_idle_times(self, current_time):
        """
        Get list of idle time periods.

        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        @return: List of idle time periods.
        @rtype: list
        """
        output = []

        idle_start_timestamp = current_time
        for record in self.__timetable:

            if idle_start_timestamp < record[0]:
                output.append(tuple([idle_start_timestamp, record[0]]))
            elif record[1] < idle_start_timestamp:
                continue
            idle_start_timestamp = record[1]

        output.append((idle_start_timestamp,))
        return output

    def get_start_timestamps(self, wall_time, current_time):
        """
        Get timestamps that correspond to the periods when job can be started.

        @param wall_time: Required processing time for the job.
        @type wall_time: float
        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float
        @return: List of tuples with timestamp and 1/-1 value (inc/dec).
        @rtype: list
        """
        output = []

        for record in self.get_idle_times(current_time=current_time):

            if len(record) == 2 and (record[1] - record[0]) < wall_time:
                continue

            output.append((record[0], 1))
            for timestamp in record[1:2]:
                output.append((timestamp - wall_time, -1))

        return output

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
                    self.__timetable.insert(idx,
                                            (start_timestamp, end_timestamp))
                    break
                elif record and end_timestamp == record[0]:
                    self.__timetable[idx] = start_timestamp, record[1]
                    break
            elif ex_record[1] == start_timestamp:
                if record is None or end_timestamp < record[0]:
                    self.__timetable[idx-1] = ex_record[0], end_timestamp
                    break
                elif record and end_timestamp == record[0]:
                    self.__timetable[idx-1] = (ex_record[0],
                                               self.__timetable.pop(idx)[1])
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
        for schedule in self.__schedules:
            cumulative_start_timestamps.append(schedule.get_start_timestamps(
                wall_time=wall_time,
                current_time=self.__current_time))

        next_timestamps = []
        for sched_id, timestamps in enumerate(cumulative_start_timestamps):
            timestamp, value = timestamps.pop(0)
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

            if cumulative_start_timestamps[sched_id]:
                sched_next_timestamp, sched_next_value = \
                    cumulative_start_timestamps[sched_id].pop(0)

                position_id = 0
                for record in next_timestamps:
                    if (sched_next_timestamp < record[0] or
                            (sched_next_timestamp == record[0] and
                             sched_id < record[1])):
                        break
                    position_id += 1
                next_timestamps.insert(position_id, (sched_next_timestamp,
                                                     sched_id,
                                                     sched_next_value))

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

    @property
    def next_scheduled_job_ids(self):
        """
        Show job ids that are scheduled to be processed next.

        @return: List of job ids.
        @rtype: list
        """
        output = []

        next_start_timestamp = self.next_start_timestamp
        if next_start_timestamp is not None:
            for record in self.__scheduled_start_data:
                if next_start_timestamp != record[0]:
                    break
                output.append(record[1])

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
        job_id = id(job)

        for sched_id in schedule_ids:
            self.__schedules[sched_id].insert(start_timestamp=start_timestamp,
                                              end_timestamp=end_timestamp)

        position_id = 0
        for record in self.__scheduled_start_data:
            if start_timestamp <= record[0]:
                break
            position_id += 1
        self.__scheduled_start_data.insert(position_id, (start_timestamp,
                                                         job_id,
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
            while (self.next_start_timestamp
                   and self.__current_time == self.next_start_timestamp):
                output.append(self.__scheduled_start_data.pop(0)[1:3])

        return output
