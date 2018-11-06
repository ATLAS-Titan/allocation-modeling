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
#

import math
import random

from qss import QSS
from qss.core.job import Job

TOTAL_NUM_NODES = 18688

ARRIVAL_RATE = 100.  # for Poisson distribution
EXECUTION_TIME_MEAN = 4.  # for Normal distribution
NODES_NUM_RATE = 1/7.  # for Poisson distribution

TIME_LIMIT = 1000.


def stream_generator(arrival_rate, execution_time, nodes_num_rate, time_limit):

    def get_random_by_poisson(rate):
        return (-1. / rate) * math.log(1. - random.random())

    def get_random_by_normal(mean_val):
        return random.normalvariate(mean_val, mean_val/2)

    next_arrival_timestamp = get_random_by_poisson(arrival_rate)
    while time_limit and next_arrival_timestamp < time_limit:

        num_nodes = int(round(get_random_by_poisson(nodes_num_rate), 0)) + 1
        yield Job(arrival_timestamp=next_arrival_timestamp,
                  execution_time=get_random_by_normal(execution_time),
                  num_nodes=num_nodes,
                  source='null')

        next_arrival_timestamp += get_random_by_poisson(arrival_rate)


if __name__ == '__main__':

    qs = QSS(num_nodes=TOTAL_NUM_NODES,
             use_queue_buffer=True,
             time_limit=TIME_LIMIT,
             output_file='qss_output.txt')

    qs.run(streams=[stream_generator(arrival_rate=ARRIVAL_RATE,
                                     execution_time=EXECUTION_TIME_MEAN,
                                     nodes_num_rate=NODES_NUM_RATE,
                                     time_limit=TIME_LIMIT)],
           verbose=True)

    print 'Utilization: {0}'.format(qs.get_utilization_value())
