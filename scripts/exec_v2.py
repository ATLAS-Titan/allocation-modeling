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

from qss import QSS, stream_generator_by_file
from qss.constants import StreamName
from qss.policy import TITAN_NUM_NODES

STREAMS_BY_FILES = [
    ('titan-logs-v2-stream-format-T-noP.csv', StreamName.External),
    ('titan-logs-v2-stream-format-P.csv', StreamName.Default)]

OUTPUT_FILE_NAME = 'qss_v2_output.txt'
TRACE_FILE_NAME = 'qss_v2_trace.txt'


if __name__ == '__main__':

    qs = QSS(num_nodes=TITAN_NUM_NODES,
             use_queue_buffer=True,
             use_scheduler=True,
             output_file=OUTPUT_FILE_NAME,
             trace_file=TRACE_FILE_NAME)

    qs.run(streams=[stream_generator_by_file(file_name=x, source=y)
                    for x, y in STREAMS_BY_FILES],
           verbose=True)

    print qs.get_utilization_value()
