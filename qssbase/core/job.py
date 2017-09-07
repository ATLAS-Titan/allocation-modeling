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


class JobSpecs(object):

    def __init__(self, arrival_timestamp):
        """
        Initialization.

        @param arrival_timestamp: Arrival timestamp (and job id).
        @type arrival_timestamp: float
        """
        self.arrival_timestamp = arrival_timestamp
        self.submission_timestamp = None
        self.release_timestamp = None

    @property
    def delay(self):
        """
        Get job delay time.

        @return: Delay time.
        @rtype: float
        """
        if self.release_timestamp:
            return self.release_timestamp - self.arrival_timestamp

    @property
    def service_delay(self):
        """
        Get job processing time.

        @return: Processing time.
        @rtype: float
        """
        if self.release_timestamp and self.submission_timestamp:
            return self.release_timestamp - self.submission_timestamp
