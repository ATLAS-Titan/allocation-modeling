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

try:
    from ..policy import JOB_POLICY
except ImportError:
    JOB_POLICY = {}


class Job(object):

    def __init__(self, execution_time, num_nodes, source_label=None, **kwargs):
        """
        Initialization.

        @param execution_time: Processing time.
        @type execution_time: float
        @param num_nodes: Number required service nodes.
        @type num_nodes: int
        @param source_label: Input stream name.
        @type source_label: str/None

        @keyword arrival_timestamp: Arrival timestamp.
        @keyword priority: Priority value.
        """
        self.execution_time = execution_time
        self.num_nodes = num_nodes

        self.source_label = source_label

        self.arrival_timestamp = kwargs.get('arrival_timestamp')
        self.submission_timestamp = None

        self.group = None
        self.priority = kwargs.get('priority', 0.)

        # priority policy
        self.set_priority_by_policy(priority_groups=JOB_POLICY.get('priority'))

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

    def set_priority_by_policy(self, priority_groups):
        """
        Set initial priority parameters (group and priority).

        @param priority_groups: List of groups with defined base priority.
        @type priority_groups: list
        """
        if not priority_groups:
            return

        for group in priority_groups:

            if not group['num_nodes_range']:
                continue

            if len(group['num_nodes_range']) < 2:
                min_num, max_num = group['num_nodes_range'][0], None
            else:
                min_num, max_num = group['num_nodes_range']

            if ((max_num and self.num_nodes > max_num)
                    or (min_num and self.num_nodes < min_num)):
                continue

            self.group = group['group']
            self.priority += group['base_priority']

    def increase_priority(self, value):
        """
        Add "aging" to the job (increase its priority).

        @param value: "Aging" value (timestamp delta).
        @type value: float
        """
        self.priority += value
