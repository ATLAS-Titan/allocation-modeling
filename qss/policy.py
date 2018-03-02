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


def priority_queue_job_init(job):
    """
    Set initial priority parameters (group and priority).

    @param job: Job object.
    @type job: qss.core.job.Job
    """
    priority_groups = {
        (1, 125): {'group': 5, 'priority': 0.},
        (126, 312): {'group': 4, 'priority': 0.},
        (313, 3749): {'group': 3, 'priority': 0.},
        (3750, 11249): {'group': 2, 'priority': 432000.},  # 5 days
        (11250,): {'group': 1, 'priority': 1296000.}  # 15 days
    }

    for k, v in priority_groups.items():
        if ((len(k) == 1 and job.num_nodes >= k[0]) or
                (len(k) == 2 and k[0] <= job.num_nodes <= k[1])):
            job.group = v['group']
            job.priority = v['priority']
            break


QUEUE_POLICY = {
    'limit': {
        StreamName.Main: 4
    },
    'discipline': QueueDiscipline.Priority,
    'job_init': priority_queue_job_init
}
