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

INPUT_FILE_NAME = 'titan-scheduler-logs-cleaned-filtered.csv'
OUTPUT_FILE_NAME = 'titan-logs-stream-format.csv'


def converter(input_file, output_file):
    """
    Converter.

    @param input_file: File name with input data (from TITAN support team).
    @type input_file: str
    @param output_file: File name for derived data (of simulator format).
    @type output_file: str
    """
    start_time = 0

    num_dropped = 0
    with open(input_file, 'r') as f_in:
        next(f_in)
        with open(output_file, 'a') as f_out:
            for line in f_in:

                # jobId;nodeCount;queueTime;startTime;endTime
                params = line.split('\r\n')[0].split(';')

                if params[3] == '0' and params[4] == '0':
                    num_dropped += 1
                    continue

                elif params[4] == '0':
                    print ('Job execution had not been finished ({0})'.
                           format(params))
                    num_dropped += 1
                    continue

                if not start_time:
                    start_time = float(params[2]) - 1

                f_out.write(','.join([
                    str(float(params[2]) - start_time),
                    str(float(params[4]) - float(params[3])),
                    str(int(params[1]))
                ]) + '\n')

    if num_dropped:
        print 'Number of dropped records: {0}'.format(num_dropped)


if __name__ == '__main__':

    converter(input_file=INPUT_FILE_NAME,
              output_file=OUTPUT_FILE_NAME)
