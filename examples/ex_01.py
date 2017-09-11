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

import math

from decimal import Decimal

from qss import QSS, stream_generator


ARRIVAL_RATE = 22./72
SERVICE_RATE = 1./3
NUM_NODES = 1000  # M/M/NUM_NODES

TIME_LIMIT = 50000.
NUM_ATTEMPTS = 10


def p_zero(n, a_rate, s_rate):
    rho = a_rate / (n * s_rate)
    return (
        Decimal(1) / (
            (((a_rate / s_rate)**n) / (math.factorial(n) * (Decimal(1) - rho)))
            + sum([
                (((a_rate / s_rate)**i) / math.factorial(i)) for i in range(n)
            ])
        )
    )


def p_q(n, a_rate, s_rate):
    rho = a_rate / (n * s_rate)
    return (
        p_zero(n, a_rate, s_rate) *
        (((a_rate / s_rate)**n) / (math.factorial(n) * (Decimal(1) - rho)))
    )


def num_jobs(n, a_rate, s_rate):
    rho = a_rate / (n * s_rate)
    return (
        ((rho * p_q(n, a_rate, s_rate)) / (Decimal(1) - rho))
        + (a_rate / s_rate)
    )


def delay(n, a_rate, s_rate):
    return (
        ((p_q(n, a_rate, s_rate)) / ((n * s_rate) - a_rate))
        + (Decimal(1) / s_rate)
    )


if __name__ == '__main__':

    qs = QSS(num_nodes=NUM_NODES)

    avg_num_jobs, max_num_jobs = 0., 0
    avg_delay = 0.
    for _ in range(NUM_ATTEMPTS):
        qs.run(stream=stream_generator(arrival_rate=ARRIVAL_RATE,
                                       execution_rate=SERVICE_RATE,
                                       num_jobs=None,
                                       time_limit=TIME_LIMIT))

        avg_num_jobs += qs.get_avg_num_jobs()
        avg_delay += qs.get_avg_delay()

        if qs.trace:
            new_max_num_jobs = max(map(lambda x: (x[1] + x[2]), qs.trace))
            if max_num_jobs < new_max_num_jobs:
                max_num_jobs = new_max_num_jobs

    print 'AVG number of jobs: {0} (max: {1}); AVG delay: {2}'.format(
        avg_num_jobs/NUM_ATTEMPTS, max_num_jobs, avg_delay/NUM_ATTEMPTS)
    print 'based on theory: AVG number of jobs: {0}; AVG delay: {1}'.format(
        num_jobs(NUM_NODES,
                 Decimal('{0}'.format(ARRIVAL_RATE)),
                 Decimal('{0}'.format(SERVICE_RATE))),
        delay(NUM_NODES,
              Decimal('{0}'.format(ARRIVAL_RATE)),
              Decimal('{0}'.format(SERVICE_RATE))))
