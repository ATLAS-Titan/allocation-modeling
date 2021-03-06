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

from .constants import StreamName
from .core.job import Job

NUM_NODES_DEFAULT = 1
SOURCE_NAME_DEFAULT = StreamName.Default


def stream_generator(arrival_rate, execution_rate, num_nodes=None,
                     source=None, label=None, first_arrival_timestamp=None,
                     num_jobs=None, time_limit=None):
    """
    Yield jobs with randomly generated arrival timestamp and execution time.

    @param arrival_rate: Arrival rate for jobs.
    @type arrival_rate: float
    @param execution_rate: Execution rate for jobs.
    @type execution_rate: float
    @param num_nodes: Number of nodes per job.
    @type num_nodes: int/None
    @param source: Name of the job's source.
    @type source: str/None
    @param label: Label (project) name.
    @type label: str/None
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

    def get_random(rate):
        return (-1. / rate) * math.log(1. - random.random())

    num_nodes_per_job = num_nodes or NUM_NODES_DEFAULT
    source = source or SOURCE_NAME_DEFAULT

    next_arrival_timestamp = first_arrival_timestamp or get_random(arrival_rate)
    while num_jobs or (time_limit and next_arrival_timestamp < time_limit):

        yield Job(arrival_timestamp=next_arrival_timestamp,
                  execution_time=get_random(execution_rate),
                  num_nodes=num_nodes_per_job,
                  source=source,
                  label=label)

        next_arrival_timestamp += get_random(arrival_rate)

        if num_jobs:
            num_jobs -= 1


def stream_generator_titan(arrival_rate, source=None, label=None,
                           first_arrival_timestamp=None,
                           num_jobs=None, time_limit=None):
    """
    Yield jobs with randomly generated parameters (Titan requirements):
       arrival timestamp, wall time, execution time, number of nodes.

    @param arrival_rate: Arrival rate for jobs.
    @type arrival_rate: float
    @param source: Name of the job's source.
    @type source: str/None
    @param label: Label (project) name.
    @type label: str/None
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

    from .policy import TITAN_REQUESTED_PROCESSOR_COUNT

    def get_random(rate):
        return (-1. / rate) * math.log(1. - random.random())

    def get_random_range(start, end):
        if start < end:
            return (-1. * random.uniform(start, end) *
                    math.log(1. - random.random()))

    source = source or SOURCE_NAME_DEFAULT
    groups = TITAN_REQUESTED_PROCESSOR_COUNT.keys()

    next_arrival_timestamp = first_arrival_timestamp or get_random(arrival_rate)
    while num_jobs or (time_limit and next_arrival_timestamp < time_limit):

        val = TITAN_REQUESTED_PROCESSOR_COUNT[random.randint(groups)]
        wall_time = (int(random.uniform(1., val['max_walltime'] / 60.)) * 60.)

        yield Job(arrival_timestamp=next_arrival_timestamp,
                  wall_time=wall_time,
                  execution_time=get_random_range(1., wall_time),
                  num_nodes=int(random.uniform(*val['nodes'])),
                  source=source,
                  label=label)

        next_arrival_timestamp += get_random(arrival_rate)

        if num_jobs:
            num_jobs -= 1


def stream_generator_by_file(file_name, source=None, time_limit=None):
    """
    Yield jobs with specified parameters from the defined file.
    (file line format:
    1. "arrivalTimestamp,executionTime,nodesCount",
    2. "arrivalTimestamp,wallTime,executionTime,nodesCount",
    3. "arrivalTimestamp,wallTime,executionTime,nodesCount,sourceName,label")

    @param file_name: File name with input data.
    @type file_name: str
    @param source: Name of the job's source.
    @type source: str/None
    @param time_limit: The maximum timestamp (until generation is done).
    @type time_limit: float/None
    @return: Job object.
    @rtype: qss.core.job.Job
    """
    if not file_name or not os.path.exists(file_name):
        raise Exception('Input data is not defined (wrong file path).')

    latest_arrival_timestamp = arrival_timestamp = 0.
    while not time_limit or (latest_arrival_timestamp < time_limit):

        with open(file_name, 'r') as f:
            for line in f:
                job_params = line.rstrip('\n').split(',')

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
                            'num_nodes': int(float(job_params[2])),
                            'source': source or SOURCE_NAME_DEFAULT})

                    elif len(job_params) > 3:
                        object_options.update({
                            #'wall_time': float(job_params[1]),  # not using
                            'execution_time': float(job_params[2]),
                            'num_nodes': int(float(job_params[3]))})

                        if source:
                            object_options['source'] = source
                        elif len(job_params) > 4:
                            object_options['source'] = job_params[4]
                        else:
                            object_options['source'] = SOURCE_NAME_DEFAULT

                        if len(job_params) > 5:
                            object_options['label'] = job_params[5]

                except ValueError:
                    continue
                else:
                    if object_options['execution_time'] == 0.0:
                        continue

                yield Job(arrival_timestamp=arrival_timestamp, **object_options)

        if not time_limit or not arrival_timestamp:
            break

        latest_arrival_timestamp = arrival_timestamp
