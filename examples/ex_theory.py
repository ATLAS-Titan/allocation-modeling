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

from decimal import Decimal


ARRIVAL_RATE = 22./72
SERVICE_RATE = 1./3
NUM_NODES = 1000  # M/M/NUM_NODES


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

    print 'AVG number of jobs: {0}; AVG delay: {1}'.format(
        num_jobs(NUM_NODES,
                 Decimal('{0}'.format(ARRIVAL_RATE)),
                 Decimal('{0}'.format(SERVICE_RATE))),
        delay(NUM_NODES,
              Decimal('{0}'.format(ARRIVAL_RATE)),
              Decimal('{0}'.format(SERVICE_RATE))))
