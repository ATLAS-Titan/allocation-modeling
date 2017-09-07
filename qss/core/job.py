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


class Job(object):

    def __init__(self, service_time, num_nodes, source_label=None, **kwargs):
        """
        Initialization.

        @param service_time: Processing time.
        @type service_time: float
        @param num_nodes: Number required service nodes.
        @type num_nodes: int
        @param source_label: Input stream name.
        @type source_label: str/None

        @keyword arrival_timestamp: Arrival timestamp.
        @keyword priority: Priority value.
        """
        self.service_time = service_time
        self.num_nodes = num_nodes

        self.source_label = source_label

        self.arrival_timestamp = kwargs.get('arrival_timestamp')
        self.submission_timestamp = None

        self.priority = kwargs.get('priority')

    @property
    def release_timestamp(self):
        """
        Get timestamp when the job will be released.

        @return: Release timestamp.
        @rtype: float
        """
        if self.submission_timestamp:
            return self.submission_timestamp + self.service_time

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
            return self.wait_time + self.service_time
