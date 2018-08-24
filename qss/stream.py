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

import math
import os
import random

from .core.job import Job

from .constants import StreamName


SOURCE_LABEL_DEFAULT = StreamName.Default
NUM_NODES_DEFAULT = 1


def stream_generator(arrival_rate, execution_rate, num_nodes=None,
                     source_label=None, first_arrival_timestamp=None,
                     num_jobs=None, time_limit=None):
    """
    Yield jobs with randomly generated arrival timestamp and execution time.

    @param arrival_rate: Arrival rate for jobs.
    @type arrival_rate: float
    @param execution_rate: Execution rate for jobs.
    @type execution_rate: float
    @param num_nodes: Number of nodes per job.
    @type num_nodes: int/None
    @param source_label: Name of the job's source.
    @type source_label: str/None
    @param first_arrival_timestamp: Initial (first) arrival timestamp.
    @type first_arrival_timestamp: float
    @param num_jobs: Number of generated jobs (None -> infinite jobs).
    @type: num_jobs: int/None
    @param time_limit: The maximum timestamp (until generation is done).
    @type time_limit: float/None
    @return: Job object.
    @rtype: qss.core.job.Job
    """
    if not num_jobs and not time_limit:
        raise Exception('Limits are not set.')

    num_nodes_per_job = num_nodes or NUM_NODES_DEFAULT
    source_label = source_label or SOURCE_LABEL_DEFAULT

    def get_random(rate):
        return (-1. / rate) * math.log(1. - random.random())

    next_arrival_timestamp = first_arrival_timestamp or get_random(arrival_rate)
    while num_jobs or (time_limit and next_arrival_timestamp < time_limit):

        yield Job(arrival_timestamp=next_arrival_timestamp,
                  execution_time=get_random(execution_rate),
                  num_nodes=num_nodes_per_job,
                  source_label=source_label)

        next_arrival_timestamp += get_random(arrival_rate)

        if num_jobs:
            num_jobs -= 1


def stream_generator_by_file(file_name, source_label=None, time_limit=None):
    """
    Yield jobs with specified parameters from the defined file.
    (file line format:
    1. "<arrival_timestamp>,<execution_time>,<num_nodes>"
    2. "<arrival_timestamp>,<wall_time>,<execution_time>,<num_nodes>")

    @param file_name: File name with input data.
    @type file_name: str
    @param source_label: Name of the job's source.
    @type source_label: str/None
    @param time_limit: The maximum timestamp (until generation is done).
    @type time_limit: float/None
    @return: Job object.
    @rtype: qss.core.job.Job
    """
    if not file_name or not os.path.exists(file_name):
        raise Exception('Input data is not defined (wrong file path).')

    source_label = source_label or SOURCE_LABEL_DEFAULT

    latest_arrival_timestamp = arrival_timestamp = 0.
    while not time_limit or (latest_arrival_timestamp < time_limit):

        with open(file_name, 'r') as f:
            for line in f:
                job_params = line.split(',')

                try:
                    arrival_timestamp = (latest_arrival_timestamp +
                                         float(job_params[0]))
                except ValueError:
                    continue
                else:
                    if time_limit and time_limit < arrival_timestamp:
                        break

                object_options = {}
                try:
                    if len(job_params) == 3:
                        object_options.update({
                            'execution_time': float(job_params[1]),
                            'num_nodes': int(float(job_params[2]))})
                    elif len(job_params) == 4:
                        object_options.update({
                            'wall_time': float(job_params[1]),
                            'execution_time': float(job_params[2]),
                            'num_nodes': int(float(job_params[3]))})
                except ValueError:
                    continue

                yield Job(arrival_timestamp=arrival_timestamp,
                          source_label=source_label,
                          **object_options)

        if not time_limit or not arrival_timestamp:
            break

        latest_arrival_timestamp = arrival_timestamp
