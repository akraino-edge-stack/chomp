#!/usr/bin/env python3

##############################################################################
# Copyright (c) 2018 AT&T Intellectual Property. All rights reserved.        #
#                                                                            #
# Licensed under the Apache License, Version 2.0 (the "License"); you may    #
# not use this file except in compliance with the License.                   #
#                                                                            #
# You may obtain a copy of the License at                                    #
#       http://www.apache.org/licenses/LICENSE-2.0                           #
#                                                                            #
# Unless required by applicable law or agreed to in writing, software        #
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT  #
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.           #
# See the License for the specific language governing permissions and        #
# limitations under the License.                                             #
##############################################################################


from datetime import datetime
import traceback

"""
akraino_logger class - provide methods to format and output log records
    in an Akraino environment. Log records written to stdout. ERROR and
    CRITICAL log records have traceback information embedded in message.
"""


class akraino_logger:

    logFormat = '%s %-8s %s(%s) %s - %s'
    timeFormat = '%Y-%m-%dT%H:%M:%S.%f'

    def output(self, t, logLevel, context, msg):

        timestamp = t.strftime(self.timeFormat)
        timestamp = timestamp[:-3]+"Z"
        toks = []
        toks = context.split('|')
        if len(toks) != 3:
            toks = []
            toks.append('n/a')
            toks.append('n/a')
            toks.append('n/a')
        module = toks[0]
        lineno = toks[1]
        func = toks[2]

        print(self.logFormat % (timestamp, logLevel, module, lineno, func, msg))

    def debug(self, message, context):

        self.output(datetime.now(), 'DEBUG', context, message)

    def info(self, message, context):

        self.output(datetime.now(), 'INFO', context, message)

    def warning(self, message, context):

        self.output(datetime.now(), 'WARNING', context, message)

    def error(self, message, context):

        msg = ('%s :traceback: %s' % (message, traceback.format_stack()))
        self.output(datetime.now(), 'ERROR', context, msg)

    def critical(self, message, context):

        msg = ('%s :traceback: %s' % (message, traceback.format_stack()))
        self.output(datetime.now(), 'CRITICAL', context, msg)
