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

import os
import sys
import redis
import datetime
import time
import json
from akrainoLogger import akraino_logger


def frameInfo(frame):
    return os.path.basename(frame.f_code.co_filename) + \
        "|" + str(frame.f_lineno) + "|" + frame.f_code.co_name


func = 'fake_data_collector'

logger = akraino_logger()
logger.info(("%s: entry" % (func)), frameInfo(sys._getframe()))

REDISHOST = '135.25.32.116'
REDISPORT = '6379'

r = redis.Redis(host=REDISHOST, port=REDISPORT, db=0)

period = 60  # seconds
while True:
    for i in range(5):
        time_start = datetime.datetime.utcnow().replace(second=0, microsecond=0)
        time_end = time_start+datetime.timedelta(seconds=period)
        time_start = time_start.strftime('%Y-%m-%dT%H:%M:%SZ')
        time_end = time_end.strftime('%Y-%m-%dT%H:%M:%SZ')

        msg = {'tstop': str(time_end), 'signature': 'sig_001', 'type': 'log', 'log': '', 'sysdata': {
               'sys_host': 'mtn21rsv002',
               'sys_component': 'kubelet'}, 'tstart': str(time_start), 'metadata': {'latency': '10m'}}
        r.publish("sig_001", json.dumps(msg))

        msg = {'tstop': str(time_end), 'signature': 'sig_002', 'type': 'log', 'log': '', 'sysdata': {
               'sys_host': 'mtn21rsv003',
               'sys_component': 'kubelet'}, 'tstart': str(time_start), 'metadata': {'latency': 15}}
        r.publish("sig_002", json.dumps(msg))
    logger.info('Publish successful for ' + str(time_start) + ' to ' + str(time_end), frameInfo(sys._getframe()))
    time.sleep(period)
