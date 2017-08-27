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


class EnumTypes(object):

    def __init__(self, *args):
        self.attrs = dict(args)
        self.values = dict(zip(range(len(args)), [x[1] for x in args]))
        self.choices = zip([x[1] for x in args], [x[0] for x in args])

    def __getattr__(self, a):
        return self.attrs[a]

    def __getitem__(self, k):
        return self.values[k]

    def __len__(self):
        return len(self.attrs)

    def __iter__(self):
        return iter(self.choices)
