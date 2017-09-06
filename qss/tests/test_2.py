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

from qss import QSS


ARRIVAL_RATE = 22./72
SERVICE_RATE = 1./3
NUM_NODES = 4  # M/M/NUM_NODES

TIME_LIMIT = 5000.
NUM_ATTEMPTS = 100


def p_zero(n, a_rate, s_rate):
    rho = a_rate / (n * s_rate)
    return (
        1. / (
            (((n * rho)**n) / (math.factorial(n) * (1. - rho))) +
            math.fsum([(((n * rho)**i) / math.factorial(i)) for i in range(n)])
        )
    )


def p_q(n, a_rate, s_rate):
    rho = a_rate / (n * s_rate)
    return (
        p_zero(n, a_rate, s_rate) *
        (((n * rho)**n) / (math.factorial(n) * (1. - rho)))
    )


def num_jobs(n, a_rate, s_rate):
    rho = a_rate / (n * s_rate)
    return (
        ((rho * p_q(n, a_rate, s_rate)) / (1. - rho)) + (n * rho)
    )


def delay(n, a_rate, s_rate):
    return (
        ((p_q(n, a_rate, s_rate)) / ((n * s_rate) - a_rate)) + (1. / s_rate)
    )


if __name__ == '__main__':

    qs = QSS(service_rate=SERVICE_RATE,
             num_nodes=NUM_NODES)

    avg_num_jobs, max_num_jobs = 0., 0
    avg_delay = 0.
    for _ in range(NUM_ATTEMPTS):
        qs.run(arrival_rate=ARRIVAL_RATE,
               time_limit=TIME_LIMIT)

        num_jobs_list = map(lambda x: (x[1] + x[2]), qs.trace)

        avg_num_jobs += (reduce(lambda x, y: x + y, num_jobs_list)
                         / float(len(qs.trace)))
        avg_delay += (reduce(lambda x, y: x + y.delay, qs.output_channel, 0.)
                      / len(qs.output_channel))

        if max_num_jobs < max(num_jobs_list):
            max_num_jobs = max(num_jobs_list)

    print 'AVG number of jobs: {0} (max: {1}); AVG delay: {2}'.format(
        avg_num_jobs/NUM_ATTEMPTS, max_num_jobs, avg_delay/NUM_ATTEMPTS)
    print 'based on theory: AVG number of jobs: {0}; AVG delay: {1}'.format(
        num_jobs(NUM_NODES, ARRIVAL_RATE, SERVICE_RATE),
        delay(NUM_NODES, ARRIVAL_RATE, SERVICE_RATE))
