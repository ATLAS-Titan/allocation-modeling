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
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2017-2018
#


class Job(object):

    def __init__(self, execution_time, num_nodes, source=None, **kwargs):
        """
        Initialization.

        @param execution_time: [Actual] processing time.
        @type execution_time: float
        @param num_nodes: Number of required service nodes.
        @type num_nodes: int
        @param source: Input source/stream name.
        @type source: str/None

        @keyword wall_time: Requested processing time.
        @keyword arrival_timestamp: Arrival timestamp.
        @keyword priority: Priority value.
        @keyword label: Label (project) name.
        """
        self.wall_time = kwargs.get('wall_time', execution_time)
        self.num_nodes = num_nodes

        self.execution_time = execution_time
        self.source = source
        self.label = kwargs.get('label')

        self.arrival_timestamp = kwargs.get('arrival_timestamp')
        self.submission_timestamp = None

        self.group = None
        self.priority = kwargs.get('priority', 0.)

    @property
    def release_timestamp(self):
        """
        Get timestamp when the job will be released.

        @return: Release timestamp.
        @rtype: float
        """
        if self.submission_timestamp:
            return self.submission_timestamp + self.execution_time

    @property
    def scheduled_release_timestamp(self):
        """
        Get timestamp of the scheduled job release (based on wall_time).

        @return: Scheduled release timestamp.
        @rtype: float
        """
        if self.submission_timestamp:
            return self.submission_timestamp + self.wall_time

    @property
    def wait_time(self):
        """
        Get job wait time (queue time).

        @return: Wait time.
        @rtype: float
        """
        if self.submission_timestamp:
            return self.submission_timestamp - self.arrival_timestamp

    @property
    def delay(self):
        """
        Get job delay time.

        @return: Delay time.
        @rtype: float
        """
        if self.arrival_timestamp and self.submission_timestamp:
            return self.wait_time + self.execution_time

    def increase_priority(self, value):
        """
        Add "aging" to the job (increase its priority).

        @param value: "Aging" value (timestamp delta).
        @type value: float
        """
        self.priority += value
