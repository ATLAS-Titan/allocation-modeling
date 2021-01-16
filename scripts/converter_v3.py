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
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2018
#

IS_ANALYSIS_FORMAT = False

if IS_ANALYSIS_FORMAT:
    OUTPUT_FORMAT = 'analysis'
else:
    OUTPUT_FORMAT = 'stream'

INPUT_FILE_NAMES = []
OUTPUT_FILE_NAME = 'titan-logs-v3-{0}-format.csv'.format(OUTPUT_FORMAT)

FTR = [3600, 60, 1]


def converter(input_files, output_file):
    """
    Converter.

    @param input_files: File names with input data (from TITAN support team).
    @type input_files: list
    @param output_file: File name for derived data (of simulator format).
    @type output_file: str
    """
    output = []

    for input_file_name in input_files:
        with open(input_file_name, 'r') as f_in:
            for line in f_in:

                params = line.split('\r\n')[0].split(' ')
                output_line = [None, None, None, None, None, None, None]

                for p in params:
                    try:
                        if 'user=' in p:
                            output_line[5] = p.split('user=')[1]
                        elif p.startswith('account='):
                            output_line[6] = p.split('=')[1]
                        elif p.startswith('qtime='):
                            output_line[0] = float(p.split('=')[1])
                        elif p.startswith('start='):
                            output_line[1] = float(p.split('=')[1])
                        elif p.startswith('end='):
                            output_line[2] = float(p.split('=')[1])
                        elif 'Resource_List.walltime' in p:
                            output_line[3] = float(
                                sum([a * b for a, b in zip(FTR, map(
                                    int, p.split('=')[1].split(':')))]))
                        elif 'Resource_List.nodes' in p:
                            output_line[4] = int(p.split('=')[1].split(':')[0])
                        # dependency -> 'Resource_List.depend'
                    except Exception, e:
                        print 'Portion: {0} | Line: {1}'.format(p, line)
                        raise Exception(e)

                if None in output_line:
                    continue

                output.append((output_line[0],
                               output_line[1],
                               ','.join([str(output_line[3]),
                                         str(output_line[2] - output_line[1]),
                                         str(output_line[4]),
                                         output_line[5],
                                         output_line[6]])))

    output.sort()

    if IS_ANALYSIS_FORMAT:
        with open(output_file, 'a') as f_out:
            for x in output:
                f_out.write(','.join([str(x[0]), str(x[1]), x[2]]) + '\n')
    else:
        init_time = output[0][0] - 1
        with open(output_file, 'a') as f_out:
            for x in output:
                f_out.write(','.join([str(x[0] - init_time), x[2]]) + '\n')


if __name__ == '__main__':

    converter(input_files=INPUT_FILE_NAMES,
              output_file=OUTPUT_FILE_NAME)
