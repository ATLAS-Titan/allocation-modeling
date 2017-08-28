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
NUM_ELEMENTS = 1000
TIME_LIMIT = 500.


if __name__ == '__main__':

    qs = QSS(service_rate=SERVICE_RATE,
             num_nodes=NUM_NODES)

    # example #1
    qs.run(arrival_rate=ARRIVAL_RATE,
           num_jobs=NUM_ELEMENTS)
    qs.print_stats()
    print 'Number of trace records: {0}, last timestamp: {1}, trace: {2}'.format(
        len(qs.trace), qs.trace[-1][0], qs.trace)

    # example #2
    qs.run(arrival_generator=random_generator(rate=ARRIVAL_RATE),
           time_limit=TIME_LIMIT)
    qs.print_stats()
    print 'Number of trace records: {0}, last timestamp: {1}, trace: {2}'.format(
        len(qs.trace), qs.trace[-1][0], qs.trace)
