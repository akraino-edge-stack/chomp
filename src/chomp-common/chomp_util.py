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

from time import sleep
from datetime import datetime


def waitfromstart(starttime, period, offset=0, delay=0):
    '''
    A function for waiting until next clock edge given a periodicity
    :param starttime: clock time in datetime.datetime object, should either be "now()" or a clock edge
    :param period: periodicity of processing
    :param offset: offset is an absolute delay from clock edge applied to all functions
    :param delay: delay is a relative delay applied for particular portions of the script
    :return:
    '''
    # performs a wait until next period time and then returns
    nexttime = getstoptime(starttime, period)
    wait = (nexttime - datetime.utcnow()).total_seconds() + delay
    if wait > 0:
        sleep(wait)
    return nexttime


def getstarttime(starttime, period, OFFSET=0):
    '''
    # gives next clock edge from starttime given a period
    # e.g. 5 min periodicity - will give closest 5 minute clock edge, 12:00, 12:05, 12:10...
    :return:
    '''
    return datetime.utcfromtimestamp((((starttime - datetime(1970, 1, 1)).total_seconds() - OFFSET)
                                      // period) * period)


def getstoptime(starttime, period, OFFSET=0):
    '''
    # gives next clock edge from starttime given a period
    # e.g. 5 min periodicity - will give closest 5 minute clock edge, 12:00, 12:05, 12:10...
    :return:
    '''
    return datetime.utcfromtimestamp((((starttime - datetime(1970, 1, 1)).total_seconds() - OFFSET)
                                      // period + 1) * period)


def addtime(time, timeadd):
    return datetime.utcfromtimestamp((time - datetime(1970, 1, 1)).total_seconds() + timeadd)


if __name__ == '__main__':
    # testing portion, this script should not get called individually
    period = 5
    delay = 0
    starttime = datetime.utcnow()
    while 1:
        print("starttime:", starttime, datetime.utcnow())
        # wait(period, delay)
        starttime = waitfromstart(starttime, period)
        # print("endtime:", datetime.utcnow())
