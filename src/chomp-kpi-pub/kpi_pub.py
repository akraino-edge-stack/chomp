#!/usr/bin/env python3

##############################################################################
# Copyright (c) 2019 AT&T Intellectual Property. All rights reserved.        #
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
import requests
import json
import time
import datetime
import signal
import queue
import threading
from akrainoLogger import akraino_logger


def frameInfo(frame):
    return os.path.basename(frame.f_code.co_filename) + \
        "|" + str(frame.f_lineno) + "|" + frame.f_code.co_name


CONFIG_DIRvar = 'CONFIG_DIR'

configDir = '/configdir'

PROPERTIES_BASE = 'kpi_pub.yaml'

logger = akraino_logger()

redisHost = ''
redisPort = 0
redisDb = 0
redisChannel = ''
headerValue = ''
waitSecs = 300

# The shutdown_flag is a threading.Event object that controls thread shutdown
shutdown_flag = threading.Event()
shutdown_flag.clear()


def listener(r, ch, q):

    '''
    This thread just subscribes messages and stores them
    from redis instance (r) and channel (ch) to queue (q)
    '''
    func = 'listener'

    try:
        pubsub = r.pubsub()
        pubsub.subscribe(ch)

        while not shutdown_flag.is_set():

            for m in pubsub.listen():
                logger.debug(("type=%s:" % (type(m['data']))), frameInfo(sys._getframe()))
                if not isinstance(m['data'], int):
                    msg = m['data'].decode("utf-8")
                    q.put(msg)
                    logger.debug(("put=%s:" % (msg)), frameInfo(sys._getframe()))

    except Exception as exc:

        shutdown_flag.set()
        logger.critical(("redis processing failure: error=%s:" % (str(exc))), frameInfo(sys._getframe()))
        logger.info(("Thread %s stopping..." % (func)), frameInfo(sys._getframe()))


class ServiceExit(Exception):

    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass


def service_shutdown(signum, frame):

    logger.info(("Caught signal %d:" % (signum)), frameInfo(sys._getframe()))
    raise ServiceExit


if __name__ == "__main__":

    func = 'kpi_pub'

    # Register the signal handlers
    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)

    execPath = os.path.dirname(os.path.realpath(sys.argv[0]))

    logger.info(("%s: entry" % (func)), frameInfo(sys._getframe()))

    # initialize
    try:

        if CONFIG_DIRvar not in os.environ:
            raise Exception("missing env var CONFIG_DIR")
        else:
            configDir = os.environ.get(CONFIG_DIRvar)

        # parse propertiesFile

        propertiesFile = PROPERTIES_BASE
        propertiesPath = os.path.join(configDir, propertiesFile)

        logger.info(("%s: propertiesPath=%s" % (func, propertiesPath)), frameInfo(sys._getframe()))

        with open(propertiesPath, 'r') as stream:
            settings = yaml.load(stream)

        logger.info(("%s: settings=%s" % (func, json.dumps(settings))), frameInfo(sys._getframe()))

        redisHost = settings['redis_host']
        redisPort = settings['redis_port']
        redisDb = settings['redis_db']
        redisChannel = settings['redis_sub_channel']

        waitSecs = settings['waitSecs']

        esProtocol = settings['es_protocol']
        esHost = settings['es_host']
        esPort = settings['es_port']
        esPathInfo = settings['es_pathinfo']
        esTimeout = settings['es_timeout']

        esUrl = esProtocol + '://' + esHost + ':' + str(esPort) + '/' + esPathInfo

    except Exception as exc:
        logger.critical(("Unable to initialize kpi_pub: error=%s:" % (str(exc))), frameInfo(sys._getframe()))
        raise SystemExit(1)

    try:
        # infinite FIFO queue
        q = queue.Queue(0)

        r = redis.StrictRedis(host=redisHost, port=redisPort, db=redisDb)
        client = threading.Thread(target=listener, args=(r, redisChannel, q))
        client.setDaemon(True)
        client.start()

        elapsed = 0

        # At some frequency, read stored records, package, POST to Akraino ES
        while not shutdown_flag.is_set():

            # let's wait several minutes to build up queue
            time.sleep(waitSecs - elapsed)

            # start time
            t0 = time.time()

            # build payload
            payload = []
            logger.debug(("queue size=%d:" % (q.qsize())), frameInfo(sys._getframe()))
            while not q.empty():
                item = q.get()
                payload.append(item)
                q.task_done()

            # Akraino POST

            """
curl -X POST -F "file=@/file-path/chomp.json" -F "origin=chomp"
-F "interval=<interval>" -F "timeStamp=<date-time-stamp>"
http://<regional-node-ip>:8080/AECPortalMgmt/chomp/upload

Akraino wants multipart-form-data:

https://stackoverflow.com/questions/12385179/how-to-send-a-multipart-form-data-with-requests-in-python

The relevant part is: file-tuple can be a 2-tuple, 3-tuple or a 4-tuple.

Based on the above, the simplest multipart form request that includes
both files to upload and form fields will look like this:

multipart_form_data = {
    'file2': ('custom_file_name.zip', open('myfile.zip', 'rb')),
    'action': ('', 'store'),
    'path': ('', '/path1')
}

response = requests.post('https://httpbin.org/post', files=multipart_form_data)

print(response.content)

Note the empty string as the first argument in the tuple for plain text fields.
This is a placeholder for the filename field which is only used for file
uploads, but for text fields the empty placeholder is still required in order
for data to be submitted.

            """

            s = json.dumps(payload)
            sz = len(s)
            ts = datetime.datetime.utcnow().replace(microsecond=0).isoformat()+'Z'
            multipartFormData = {
                'file': ('chomp.json', s),      # file
                'originSrc': ('', 'chomp'),     # text
                'interval': ('', '5'),          # text
                'timeStamp': ('', ts)           # text
                }

            try:
                req = requests.post(esUrl,
                                    files=multipartFormData,
                                    timeout=esTimeout)
                logger.info(("POST: status code=%d %s,headers=%s:" % (req.status_code, req.reason, req.headers)),
                            frameInfo(sys._getframe()))

                if req.status_code != 200:
                    logger.error(("Failed to post to Akraino, code=%d:" % (req.status_code)),
                                 frameInfo(sys._getframe()))

            except Exception as exc:
                logger.error(("Failed to post to Akraino, error=%s:" % (str(exc))), frameInfo(sys._getframe()))

            # finished with publishing and posting, how much time left?
            elapsed = round(time.time() - t0)

    except ServiceExit:
        shutdown_flag.set()
        client.join()

    logger.critical(("%s: exit" % (func)), frameInfo(sys._getframe()))
    logger.info(("client thread alive?"+str(client.is_alive())), frameInfo(sys._getframe()))

    exit(0)
