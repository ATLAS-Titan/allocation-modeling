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

from qss import QSS, random_generator


ARRIVAL_RATE = 10./9
SERVICE_RATE = 1./3
NUM_NODES = 3  # M/M/NUM_NODES
NUM_ELEMENTS = 10000
TIME_LIMIT = 5000.


if __name__ == '__main__':

    qs = QSS(service_rate=SERVICE_RATE,
             num_nodes=NUM_NODES)

    qs.run(arrival_times=[random_generator(ARRIVAL_RATE)
                          for _ in range(NUM_ELEMENTS)],
           time_limit=TIME_LIMIT)

    qs.print_stats()

    print qs.trace
