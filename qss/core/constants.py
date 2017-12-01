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

from ..utils import EnumTypes


ActionCode = EnumTypes(
    ('Arrival', 'a'),
    ('Submission', 's'),
    ('Completion', 'c'),
)

ServiceState = EnumTypes(
    ('Arrival', 0),
    ('Completion', 1),
    ('Stop', 2)
)

QueueDiscipline = EnumTypes(
    ('FIFO', 'fifo'),
    ('Priority', 'priority')
)
