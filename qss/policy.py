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

from .constants import StreamName


QUEUE_POLICY = {
    'limit': {
        StreamName.Main: 4
    }
}

JOB_POLICY = {
    'priority': [
        {'num_nodes_range': (1, 125),
         'group': 5,
         'base_priority': 0.},

        {'num_nodes_range': (126, 312),
         'group': 4,
         'base_priority': 0.},

        {'num_nodes_range': (313, 3749),
         'group': 3,
         'base_priority': 0.},

        {'num_nodes_range': (3750, 11249),
         'group': 2,
         'base_priority': 432000.},  # 5 days

        {'num_nodes_range': (11250,),
         'group': 1,
         'base_priority': 1296000.}  # 15 days
    ]
}
