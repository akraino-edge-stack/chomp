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
import yaml
import redis
import datetime
import json
import time
import statistics
from akrainoLogger import akraino_logger


def frameInfo(frame):
    return os.path.basename(frame.f_code.co_filename) + \
        "|" + str(frame.f_lineno) + "|" + frame.f_code.co_name


def rm_unit(s):

    if not isinstance(s, str):
        return s
    size = len(s)
    if not s or size < 2:
        return
    unit = s[-1:]
    if unit.isdigit():
        return float(s)
    elif unit == 'm':
        return float(s[0:size-1])/1000
    elif unit == 'u':
        return float(s[0:size-1])/1000000


def counter_module():

    # Get configuration parameters
    configDir = os.environ.get("CONFIG_DIR")
    try:
        fqConf = configDir + "/" + "singlelog_count.yaml"

        with open(fqConf, 'r') as f:
            config_dic = yaml.load(f)

        redisHost = config_dic['redis_host']
        redisPort = config_dic['redis_port']
        redisDb = config_dic['redis_db']
        period = config_dic['period']
        offset = config_dic['offset']
        countersFile = config_dic['countersfile']  # counters.json

    except Exception:
        logger.error(('Failed to read the configuration file %s.' % fqConf), frameInfo(sys._getframe()))

    logger.debug(('configuration=%s' % config_dic), frameInfo(sys._getframe()))

    # Load counters json file
    try:
        counters_dic = {}
        fqCountersFile = configDir + '/' + countersFile
        with open(fqCountersFile) as f:
            for counter in f:
                fqCounter = configDir + '/' + counter.lstrip().rstrip()
                with open(fqCounter) as f2:
                    counter_dic = json.load(f2)
                    counters_dic[counter_dic['name']] = counter_dic
    except Exception:
        logger.error(('Failed to decode counters JSON file %s.' % fqCountersFile), frameInfo(sys._getframe()))
        return

    # Create signatures_dic dictionary with list of signatures as keys
    signatures_dic = {}
    for counter in counters_dic:
        if counters_dic[counter]['type'] == 'single':
            for signature in counters_dic[counter]['signatures']:
                if signature not in signatures_dic:
                    signatures_dic[signature] = {}

    # Initialize a redis object
    try:
        r = redis.Redis(host=redisHost, port=redisPort, db=redisDb)
        p = r.pubsub()
    except Exception:
        logger.error('Failed to connect to redis channels.', frameInfo(sys._getframe()))
        return

    # Subscribe to redis channels with names in signaturelist
    try:
        p.subscribe(signatures_dic.keys())
        logger.info('Subscribed to redis channels.', frameInfo(sys._getframe()))
    except Exception:
        logger.error('Failed to subscribe to redis channels.', frameInfo(sys._getframe()))
        return

    time.sleep(2*period*60)

    # Calculate end_time: T is the start of current period
    T = datetime.datetime.utcnow().replace(microsecond=0, second=0)
    T -= datetime.timedelta(T.minute % period)

    # Calculate end_time, the end time of the period we will process which is end_time = T - offset
    end_time = (T-datetime.timedelta(minutes=(offset+period)))

    # Calculate start_time, the start time of the period
    # we will process which is start_time = T-offset-period
    start_time = end_time-datetime.timedelta(minutes=period)

    # Count messages observed between start_time and end_time
    skipped = False
    skipped_sig = ''
    skipped_latency = []
    skipped_log = []
    while True:
        # Intialize signature counts to 0 and latency to empty lists
        for signature in signatures_dic:
            signatures_dic[signature]['count'] = 0
            signatures_dic[signature]['latency'] = []
            signatures_dic[signature]['log'] = []
        # Count a message seen of this period in previous iteration;
        # append latency of seen message of this period in previous iteration.
        if skipped is True:
            signatures_dic[skipped_sig]['count'] = 1
            signatures_dic[skipped_sig]['latency'].append(rm_unit(skipped_latency))
            signatures_dic[skipped_sig]['log'].append(skipped_log)

        # Keep getting messages until either (1) no messages are at
        # the subscriber or (2) observed a message belonging to next period
        while True:

            m = p.get_message()
            if not m:
                break

            # skip redis type 'subscribe' message
            if m['type'] == 'subscribe':
                continue

            # Get message in data
            message = str(m['data'])
            logger.debug('message = ' + message, frameInfo(sys._getframe()))

            message_json = {}
            try:
                message_json = json.loads(message)

            except Exception:
                logger.warning('message is not JSON. message = ' +
                               message, frameInfo(sys._getframe()))
                continue

            # Increment count and append latency of all signatures in
            # signatures_dic based on signature field in message.
            # Upon encoutering a message for the next interval,
            # flag skipped and break

            message_start_time = datetime.datetime.strptime(message_json['tstart'], '%Y-%m-%dT%H:%M:%SZ')
            if message_start_time == start_time:
                signatures_dic[message_json['signature']]['count'] += 1
                signatures_dic[message_json['signature']]['latency'] \
                    .append(rm_unit(message_json['metadata']['latency']))
                signatures_dic[message_json['signature']]['log'] \
                    .append([message_json])

            elif message_start_time > start_time:
                skipped = True
                skipped_sig = message_json['signature']
                skipped_latency = message_json['metadata']['latency']
                skipped_log = [message_json]
                break

        # Create outputs of current period
        for counter in counters_dic:
            output = {}
            output['tstart'] = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            output['tstop'] = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            output['name'] = counter
            output['type'] = counters_dic[counter]['type']
            output['latency'] = []
            output['log'] = []
            count = 0
            for signature in counters_dic[counter]['signatures']:
                count += signatures_dic[signature]['count']
                output['latency'].extend(signatures_dic[signature]['latency'])
                output['log'].extend(signatures_dic[signature]['log'])
            if output['latency']:
                output['latency_max'] = max(output['latency'])
                output['latency_min'] = min(output['latency'])
                output['latency_avg'] = statistics.mean(output['latency'])
            else:
                output['latency_max'] = None
                output['latency_min'] = None
                output['latency_avg'] = None

            output['count'] = count

            # Publish output
            try:
                output_str = json.dumps(output)
                r.publish("counters", output_str)
                logger.info('Successfully published output = ' + output_str, frameInfo(sys._getframe()))
            except Exception:
                logger.error(str(datetime.datetime.utcnow()) +
                             ' Failed to publish output of counter for the period: ' +
                             output['tstart'] + ' --- ' + output['tstop'] +
                             '.', frameInfo(sys._getframe()))

        # Update start_time and end_time to next period
        start_time = end_time
        end_time = start_time + datetime.timedelta(minutes=period)
        time.sleep(period*60)


if __name__ == "__main__":

    func = 'singlelog_count'

    logger = akraino_logger()
    logger.info(("%s: entry" % (func)), frameInfo(sys._getframe()))

    counter_module()
