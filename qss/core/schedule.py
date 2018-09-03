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
        self.__timetable = []  # (<start_time>, <end_time>, <job_id>)
        self.__busy_times_cached = None

    def reset(self):
        """
        Reset internal parameters.
        """
        del self.__timetable[:]
        self.__busy_times_cached = None

    def set_initial_busy_time(self, start_timestamp, end_timestamp, **kwargs):
        """
        Set the first busy time period (that is currently ongoing).

        @param start_timestamp: Scheduled start time for the job processing.
        @type start_timestamp: float
        @param end_timestamp: Scheduled end time of the job processing.
        @type end_timestamp: float

        @keyword job_id: Job id.
        """
        self.reset()

        record = [start_timestamp, end_timestamp]
        if kwargs.get('job_id') is not None:
            record.append(kwargs['job_id'])

        self.__timetable.append(tuple(record))

    @property
    def busy_times(self):
        """
        List of busy time periods.

        @return: List of busy time periods.
        @rtype: list
        """
        if self.__busy_times_cached is None:
            self.__busy_times_cached = []

            if self.__timetable:
                iterator = iter(self.__timetable[1:])
                busy_period = list(self.__timetable[0][:2])
                while True:

                    try:
                        record = iterator.next()
                    except StopIteration:
                        record = None

                    if record and busy_period[1] == record[0]:
                        busy_period[1] = record[1]
                    else:
                        self.__busy_times_cached.append(tuple(busy_period))
                        if record:
                            del busy_period[:]
                            busy_period.extend(record[:2])
                        else:
                            break

        return self.__busy_times_cached

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
        for record in self.busy_times:

            if idle_start_timestamp < record[0]:
                output.append(tuple([idle_start_timestamp, record[0]]))
            elif record[1] < idle_start_timestamp:
                continue
            idle_start_timestamp = record[1]

        output.append(tuple([idle_start_timestamp]))
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

    def insert(self, start_timestamp, end_timestamp, job_id):
        """
        Add record to the schedule for the Node object.

        @param start_timestamp: Scheduled start time for the job processing.
        @type start_timestamp: float
        @param end_timestamp: Scheduled end time of the job processing.
        @type end_timestamp: float
        @param job_id: Job id.
        @type job_id: int
        """
        position_id, previous_end_timestamp = 0, 0.
        for record in self.__timetable:

            if (previous_end_timestamp <= start_timestamp
                    and end_timestamp <= record[0]):
                break

            elif previous_end_timestamp > start_timestamp:
                raise Exception('New record cannot fit to the idle period.')

            position_id += 1
            previous_end_timestamp = record[1]

        self.__timetable.insert(position_id, tuple([start_timestamp,
                                                    end_timestamp,
                                                    job_id]))
        self.__busy_times_cached = None


class ScheduleManager(object):

    def __init__(self, num_nodes):
        """
        Initialization.

        @param num_nodes: Number of service nodes.
        @type num_nodes: int
        """
        self.__schedules = [NodeSchedule() for _ in range(num_nodes)]
        self.__num_idle_nodes_now = num_nodes
        self.__current_time = 0.

    def __get_schedule_parameters(self, job):
        """
        Get parameters to insert the job into schedules.

        @param job: Job object.
        @type job: qss.core.job.Job
        @return: Start_timestamp and list of schedule ids.
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

        # initialization for all schedules' start_timestamps
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

    @staticmethod
    def get_backfill_max_element_id(queue_iterator, num_idle_nodes):
        """
        Get the max element id in the queue that is a potential backfill job.

        @param queue_iterator: Queue iterator.
        @type queue_iterator: iterator
        @param num_idle_nodes: The number of idle nodes (available for jobs).
        @type num_idle_nodes: int
        @return: Element [max] id in the queue.
        @rtype: int/None
        """
        output = None

        for eid, job in enumerate(queue_iterator):
            if num_idle_nodes >= job.num_nodes:
                output = eid

        return output

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
            if sched_id in node_release_timestamps:
                schedule.set_initial_busy_time(
                    start_timestamp=self.__current_time,
                    end_timestamp=node_release_timestamps[sched_id])
            else:
                schedule.reset()

        self.__num_idle_nodes_now = \
            len(self.__schedules) - len(node_release_timestamps)

    def get_backfill_elements(self, queue_iterator, current_time=None):
        """
        Get elements (ids) for backfill mode.

        @param queue_iterator: Queue iterator.
        @type queue_iterator: iterator
        @param current_time: Current time (timestamp from 0 to now).
        @type current_time: float/None
        @return: List of ids (eid, job_id).
        @rtype: list
        """
        if current_time is not None:
            self.__current_time = current_time

        output = []

        if self.__num_idle_nodes_now:
            for eid, job in enumerate(queue_iterator):

                start_timestamp, schedule_ids = \
                    self.__get_schedule_parameters(job=job)

                if start_timestamp:
                    end_timestamp = start_timestamp + job.wall_time
                    job_id = id(job)

                    for sched_id in schedule_ids:
                        self.__schedules[sched_id].insert(
                            start_timestamp=start_timestamp,
                            end_timestamp=end_timestamp,
                            job_id=job_id)

                    if start_timestamp == self.__current_time:
                        output.append((eid, job_id))
                        self.__num_idle_nodes_now -= len(schedule_ids)

                if not self.__num_idle_nodes_now:
                    break

        return output
