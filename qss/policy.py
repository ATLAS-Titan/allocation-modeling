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
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2017-2018
#

from .constants import StreamName
from .core import QueueDiscipline

TITAN_NUM_NODES = 18688
TITAN_REQUESTED_PROCESSOR_COUNT = {
    1: {'nodes': (11250, TITAN_NUM_NODES),
        'max_walltime': 86400.,
        'aging_boost': 1296000.},
    2: {'nodes': (3750, 11249),
        'max_walltime': 86400.,
        'aging_boost': 432000.},
    3: {'nodes': (313, 3749),
        'max_walltime': 43200.,
        'aging_boost': 0.},
    4: {'nodes': (126, 312),
        'max_walltime': 21600.,
        'aging_boost': 0.},
    5: {'nodes': (1, 125),
        'max_walltime': 7200.,
        'aging_boost': 0.},
}


def priority_queue_job_init(job):
    """
    Set initial priority parameters (group and priority).

    @param job: Job object.
    @type job: qss.core.job.Job
    """
    if job.num_nodes > TITAN_NUM_NODES:
        raise Exception('Number of requested nodes exceeds the total number.')

    for k, v in TITAN_REQUESTED_PROCESSOR_COUNT.iteritems():
        if v['nodes'][0] <= job.num_nodes <= v['nodes'][1]:
            job.group = k
            job.priority = v['aging_boost']
            break


QUEUE_POLICY = {
    'limit': {
        #StreamName.Main: 4,
        '_per_source': 4
    },
    'discipline': QueueDiscipline.Priority,
    'job_init': priority_queue_job_init
}
