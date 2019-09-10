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
from json import loads, dumps
import chomp_util as util
from datetime import datetime
from akrainoLogger import akraino_logger


def frameInfo(frame):
    return os.path.basename(frame.f_code.co_filename) + \
        "|" + str(frame.f_lineno) + "|" + frame.f_code.co_name


logger = akraino_logger()


def chomp_redis_pub(host, port, q, lock, period, delay=''):

    logger.info(q, frameInfo(sys._getframe()))
    '''
    Publishes data to redis from a queue that stores the data
    :param host:
    :param port:
    :param q:
    :param lock:
    :param period:
    :return:
    '''
    if delay == '':
        delay = period/2
    publisher = redis.Redis(host=host, port=port, db=0)
    while True:
        # wait
        util.waitfromstart(datetime.utcnow(), period, delay=delay)
        messages = []
        lock.acquire()
        while not q.empty():
            messages.append(q.get())
        lock.release()

        for message in messages:
            topic = message['signature']
            publisher.publish(topic, dumps(message))


def chomp_redis_sub(host, port, signaturelist, period, q, lock, delay=0):

    # Subscribe from redis
    try:
        a = redis.Redis(host=host, port=port, db=0)
        subscriber = a.pubsub()
        subscriber.subscribe(signaturelist)
    except Exception as e:
        logger.error("redis subscribe failed: {}"
                     .format(str(e)),
                     frameInfo(sys._getframe()))
        raise

    messages = []
    util.waitfromstart(datetime.utcnow(), period, delay)
    while True:
        message = subscriber.get_message()
        if isinstance(message, type(None)):
            logger.info("loading messages to q",
                        frameInfo(sys._getframe()))
            lock.acquire()
            # for message in messages:
            for message in sorted(messages, key=lambda k: k['log']):
                # q.put(message['data'].decode("utf-8"))
                q.put(message)
            lock.release()
            messages = []
            util.waitfromstart(datetime.utcnow(), period, delay)
            continue
        try:
            # messages.append(loads(message['data'].decode("utf-8")))
            temp = loads(message['data'].decode("utf-8"))
            if temp in messages:
                logger.info("message already exists in messages list: {}"
                            .format(temp),
                            frameInfo(sys._getframe()))
            # TODO: should this be under "else:"?
            messages.append(loads(message['data'].decode("utf-8")))

        except Exception:
            logger.info("received message is not dict: {}"
                        .format(message),
                        frameInfo(sys._getframe()))
            pass


def get_signature_list(signatures, type):
    signaturelist = []
    for signature in signatures:
        if type in signature['type']:
            signaturelist.append(signature['name'])
    return signaturelist


def check_message_tstamp(message):
    if 'type' not in message.keys():
        return True
    if message['type'].lower() != "log":
        return True

    # if message['tstop']
