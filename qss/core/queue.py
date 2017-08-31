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


class ServiceQueue(object):

    def __init__(self, limit=None):
        """
        Initialization.

        @param limit: Maximum number of elements in queue.
        @type limit: int/None
        """
        self.__data = []
        self.__limit = limit

        self.__num_dropped = 0.

    @property
    def length(self):
        """
        Get the number of elements in queue.

        @return: Number of elements.
        @rtype: int
        """
        return len(self.__data)

    @property
    def is_empty(self):
        """
        Flag shows whether queue is empty or not.

        @return: Flag that queue is empty.
        @rtype: bool
        """
        return True if not self.__data else False

    @property
    def num_dropped(self):
        """
        Get the number of dropped elements.

        @return: Number of dropped elements.
        @rtype: int
        """
        return self.__num_dropped

    def reset(self):
        """
        Reset parameters.
        """
        del self.__data[:]
        self.__num_dropped = 0.

    def add(self, element):
        """
        Add element to the queue.

        @param element: Queue element.
        @type element: -
        """
        if not self.__limit or self.__limit > self.length:
            self.__data.append(element)
        elif self.__limit:
            self.__num_dropped += 1

    def pop(self):
        """
        Get (remove and return) element from the queue.

        @return: Queue element.
        @rtype: -
        """
        return self.__data.pop(0)
