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
# - Alexey Poyda, <poyda@wdcb.ru>, 2017
#

from qss import QSS, stream_generator


ARRIVAL_RATE = 22./72
SERVICE_RATE = 1./3
NUM_NODES = 100  # M/M/NUM_NODES

TIME_LIMIT = 100.
NUM_ATTEMPTS = 2


if __name__ == '__main__':

    qs = QSS(num_nodes=NUM_NODES)

    max_num_jobs, avg_num_jobs, avg_delay = 0, 0., 0.

    for _ in range(NUM_ATTEMPTS):

        qs.run(streams=[
            stream_generator(arrival_rate=ARRIVAL_RATE/2,
                             execution_rate=SERVICE_RATE,
                             num_nodes=1,
                             source_label='external',
                             num_jobs=None,
                             time_limit=TIME_LIMIT),
            stream_generator(arrival_rate=ARRIVAL_RATE/2,
                             execution_rate=SERVICE_RATE,
                             num_nodes=100,
                             source_label='main',
                             num_jobs=None,
                             time_limit=TIME_LIMIT)
        ])

        avg_num_jobs += qs.get_avg_num_jobs()
        avg_delay += qs.get_avg_delay()

        if qs.trace:
            new_max_num_jobs = max(map(lambda x: (x[1] + x[2]), qs.trace))
            if max_num_jobs < new_max_num_jobs:
                max_num_jobs = new_max_num_jobs

        print 'Output:', map(lambda x: x.source_label, qs.output_channel)

    print 'AVG number of jobs: {0} (max: {1}); AVG delay: {2}'.format(
        avg_num_jobs/NUM_ATTEMPTS, max_num_jobs, avg_delay/NUM_ATTEMPTS)
