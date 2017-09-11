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

import math
import random

from .job import Job


SOURCE_LABEL_DEFAULT = 'main'
NUM_NODES_DEFAULT = 1


def stream_generator(arrival_rate, execution_rate, num_jobs=None, time_limit=None):
    """
    Yield jobs with randomly generated arrival and service times.

    @param arrival_rate: Arrival rate for jobs.
    @type arrival_rate: float
    @param execution_rate: Execution rate for jobs.
    @type execution_rate: float
    @param num_jobs: Number of generated jobs (None -> infinite jobs).
    @type: num_jobs: int/None
    @param time_limit: The maximum timestamp (until generation is done).
    @type time_limit: float/None
    @return: Job object.
    @rtype: float
    """
    if not num_jobs and not time_limit:
        raise Exception('Limits are not set.')

    def get_random(rate):
        return (-1. / rate) * math.log(1. - random.random())

    next_arrival_timestamp = get_random(arrival_rate)
    while num_jobs or (time_limit and next_arrival_timestamp < time_limit):

        yield Job(execution_time=get_random(execution_rate),
                  arrival_timestamp=next_arrival_timestamp,
                  source_label=SOURCE_LABEL_DEFAULT,
                  num_nodes=NUM_NODES_DEFAULT)

        next_arrival_timestamp += get_random(arrival_rate)

        if num_jobs:
            num_jobs -= 1
