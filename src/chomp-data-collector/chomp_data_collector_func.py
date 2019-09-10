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
from datetime import datetime
from json import loads, dumps
from multiprocessing import Queue, Process, Lock
from time import sleep
import chomp_config_load as cfg
import chomp_redis
import chomp_util as util
import requests as req
import re
from akrainoLogger import akraino_logger


def frameInfo(frame):
    return os.path.basename(frame.f_code.co_filename) + \
        "|" + str(frame.f_lineno) + "|" + frame.f_code.co_name


appconfigfile = ''
siglistfile = './signaturelist'

DATA_Q = Queue()
QUERY_Q = Queue()
LOCK = Lock()


def esquery(signature, tstart, tstop, appconfig, q, lock):

    '''
    Performs an elasticsearch query based on tstart/tstop and query signature from signature file
    :param signature: signature dict from list of signatures loaded during initialization
    :param appconfig: application config dict from application config loaded during initialization
    :param tstart: time start edge as datetime.datetime object
    :param tstop: time stop edge as datetime.datetime object
    :param q: q for storing final data
    :param lock: lock for protecting q.put
    :return:
    '''

    # form json for elasticsearch query
    # log?
    jsonquery = signature['query'].copy()
    _esquery_add_timestamp(jsonquery, tstart, tstop)
    _esquery_add_size(jsonquery, signature)
    logger.info("query for {}-{}, signature {} starting\nquery: {}"
                .format(tstart, tstop, signature['name'], jsonquery),
                frameInfo(sys._getframe()))

    # use the api
    response = post_es(appconfig, jsonquery)
    entries = format_esquery_output(response, signature)

    # http response code was not 200, and was thus an error
    if len(entries) > 0 and 'error' in entries[0].keys():
        logger.warning("query error {}"
                       .format(entries),
                       frameInfo(sys._getframe()))
        return

    lock.acquire()
    logger.info("adding entries to publish", frameInfo(sys._getframe()))
    # for entry in entries:
    for entry in sorted(entries, key=lambda k: k['log']):
        dataentry = entry.copy()
        dataentry['signature'] = signature['name']
        dataentry['tstart'] = tstart.strftime("%Y-%m-%dT%H:%M:%SZ")
        dataentry['tstop'] = tstop.strftime("%Y-%m-%dT%H:%M:%SZ")
        q.put(dataentry)
        logger.debug(dumps(dataentry), frameInfo(sys._getframe()))
    lock.release()
    # log?
    logger.info("for {}-{},{} completed"
                .format(tstart, tstop, signature['name']),
                frameInfo(sys._getframe()))
    return


def post_es(appconfig, jsonquery):
    response = ''
    timeout = 1
    if 'query_timeout' in appconfig.keys():
        timeout = appconfig['query_timeout']
    # form inputs to http POST call
    headers = {'content-type': 'application/json'}
    # password stuff here as part of headers?
    if 'es_host' not in appconfig.keys():
        return {}
    posturl = "http://" + appconfig['es_host']
    if 'es_port' in appconfig.keys():
        if appconfig['es_port'] != '':
            posturl += ':' + appconfig['es_port']
    posturl += '/_search'

    httpauth = None
    if 'es_user' in appconfig.keys() and 'es_pswd' in appconfig.keys():
        if appconfig['es_user'] != '' and appconfig['es_pswd'] != '':
            httpauth = req.auth.HTTPBasicAuth(appconfig['es_user'], appconfig['es_pswd'])

    try:
        if httpauth:
            response = req.post(posturl, auth=httpauth, headers=headers, data=dumps(jsonquery), timeout=timeout)
        else:
            response = req.post(posturl, headers=headers, data=dumps(jsonquery), timeout=timeout)
        return response
    except req.exceptions.ConnectTimeout:
        logger.warning("request.post has timed out", frameInfo(sys._getframe()))
        return "post_es() request.post has timed out"
    except Exception as exc:
        logger.error("request.post has failed for unknown reason, err={}"
                     .format(str(exc)),
                     frameInfo(sys._getframe()))
        return "post_es() request.post has failed for unknown reason"


def format_esquery_output(response, signature):

    '''
    format elasticsearch return into individual json docs with metadata
    :param response:
    :return:
    '''
    responsedict = {}
    entries = []
    sysdata_keys = {}
    sysdata_keys['log'] = {
        'container': 'container_name',
        'pod': 'pod_name',
        'namespace': 'namespace_name',
        'host': 'host'
    }
    sysdata_keys['message'] = {
        'component': 'ident',
    }

    if isinstance(response, str):
        # error during request, no response received
        return [{'error':  response}]

    try:
        if response.status_code != 200:
            logger.warning("response error {}"
                           .format(response.text),
                           frameInfo(sys._getframe()))
            return [{'error':  str(response.text)}]
    except Exception:
        pass

    try:
        responsedict = loads(response.text)
    except Exception:
        return [{'error':  str(response)}]

    for record in responsedict['hits']['hits']:
        sysdata = {}
        if 'log' in record['_source'].keys():
            log = record['_source']['log']
            for key in sysdata_keys['log'].keys():
                try:
                    # for k8s logs fluentd/bit add metadata to 'kubernetes' key
                    sysdata[key] = record['_source']['kubernetes'][sysdata_keys['log'][key]]
                except Exception:
                    pass
            metadata = _esquery_regex(log, signature)

        elif 'message' in record['_source'].keys():
            log = record['_source']['message']
            for key in sysdata_keys['message'].keys():
                try:
                    # for syslog, fluentd/bit adds metadata directly to _source
                    sysdata[key] = record['_source'][sysdata_keys['message'][key]]
                except Exception:
                    pass
            metadata = _esquery_regex(log, signature)

        if metadata == 'SKIP':
            continue
        entry = {'log': log, 'sysdata': sysdata, 'metadata': metadata}
        entries.append(entry)

    return entries


def _esquery_regex(log, signature):
    '''
    this function applies regex expressions as indicated by a signature file
    to the log.
    '''
    metadata = {}
    if 'regex' in signature.keys():
        for key in signature['regex'].keys():
            try:
                temp = re.match(signature['regex'][key], log)
                # temp = temp.groups()
                metadata[key] = temp.groups()[0]
            except Exception:
                metadata[key] = ''
                # return "SKIP"

    return metadata


def _esquery_add_timestamp(jsonquery, tstart, tstop):
    '''
    adding a time to the es jsonquery
    :param jsonquery:
    :param tstart:
    :param tstop: datetime.datetime object specifying end of window
    :return:
    '''
    if 'query' not in jsonquery.keys():
        jsonquery['query'] = {}
    if 'bool' not in jsonquery['query'].keys():
        jsonquery['query']['bool'] = {}
    if 'must' not in jsonquery['query']['bool'].keys():
        jsonquery['query']['bool']['must'] = []
    timerange_es = {"range": {"@timestamp": {"gte": tstart.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                                             "lt": tstop.strftime("%Y-%m-%dT%H:%M:%S.%f")}}}
    jsonquery['query']['bool']['must'].append(timerange_es)
    return


def _esquery_add_size(jsonquery, signature):
    '''
    Add the "size" component to es query
    :param jsonquery:
    :param signature:
    :return:
    '''
    try:
        jsonquery['size'] = 10000
    except Exception:
        jsonquery['size'] = 0
    return


def populate_query_q(q, lock, signatures, period, OFFSET=0):
    '''
    populates queries with current time to next time window
    updated to be non-recursive
    :param q:
    :param lock:
    :param tstart:
    :param period: periodicity
    :param signatures:
    :return:
    '''
    while 1:
        currenttime = datetime.utcnow()
        tstart = util.getstarttime(currenttime, period, OFFSET)
        tstop = util.getstoptime(currenttime, period, OFFSET)
        logger.info("populating queries for {} to {}"
                    .format(tstart, tstop),
                    frameInfo(sys._getframe()))
        for signature in signatures:
            entry = [signature, tstart, tstop]
            logger.info("added query for signature {}, {}-{}"
                        .format(signature['name'], tstart, tstop),
                        frameInfo(sys._getframe()))
            lock.acquire()
            q.put(entry)
            lock.release()
        util.waitfromstart(datetime.utcnow(), period, delay=period-5)


def manage_esquery(appconfig, queryq, dataq, lock):
    # arguments for esquery(): signature, tstart, tstop, appconfig, q, lock
    queryproclist = []
    proclist = []
    starttime = util.waitfromstart(datetime.utcnow(), appconfig['period'])
    while 1:
        logger.debug("process list kick off and population", frameInfo(sys._getframe()))
        lock.acquire()
        while not queryq.empty():
            queryproclist.append(queryq.get())
        lock.release()

        while 1:
            for i in reversed(range(len(proclist))):
                if not proclist[i].is_alive():
                    proclist.pop(i)
            if len(queryproclist) == 0:
                break
            if len(proclist) == appconfig['query_proc_count']:
                sleep(.1)
                continue
            for i in range(appconfig['query_proc_count']-len(proclist)):
                if len(queryproclist) == 0:
                    break
                proclist.append(Process(target=esquery, args=[queryproclist[0][0], queryproclist[0][1],
                                                              queryproclist[0][2], appconfig, dataq, lock]))
                proclist[-1].start()
                queryproclist.pop(0)

        logger.info("all processes kicked off, awaiting next set", frameInfo(sys._getframe()))
        starttime = util.waitfromstart(starttime, appconfig['period'])


if __name__ == '__main__':
    # main function and runner

    func = 'data_collector'

    logger = akraino_logger()
    logger.info(("%s: entry" % (func)), frameInfo(sys._getframe()))

    if len(sys.argv) < 2:
        appconfigfile = cfg.getconfigfile(func)
    else:
        appconfigfile = cfg.getconfigfile(sys.argv[1])

    appconfig = cfg.load_config(appconfigfile)
    logger.info(str(appconfig), frameInfo(sys._getframe()))

    signatures = cfg.load_signatures_siglist(appconfig['siglistfile'])
    if 'regexfile' in appconfig.keys():
        signatures = cfg.load_regex(signatures, appconfig['regexfile'])

    queries = []
    pop_q_process = Process(target=populate_query_q,
                            args=[QUERY_Q, LOCK, signatures, appconfig['period'],
                                  appconfig['offset']])
    pop_q_process.start()

    # arguments for manage_esquery (appconfig, queryq, dataq, lock)
    manage_esquery_process = Process(target=manage_esquery,
                                     args=[appconfig, QUERY_Q, DATA_Q, LOCK])
    manage_esquery_process.start()

    publish_process = Process(target=chomp_redis.chomp_redis_pub,
                              args=[appconfig['redis_host'],
                                    appconfig['redis_port'],
                                    DATA_Q, LOCK, appconfig['period'],
                                    appconfig['data_pub_delay']])
    publish_process.start()
    publish_process.join()
