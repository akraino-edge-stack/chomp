#!/usr/bin/env python3

from datetime import datetime
# from random import randint
from json import loads, dumps
from multiprocessing import Queue, Process, Lock
from time import sleep
import chomp_config_load as cfg
import chomp_redis
import chomp_util as util
from requests import post
import requests.auth as auth
from sys import argv
import re
import corrlog_test_data as simdata

# import chomp_env_var as env

# REDISHOST='135.25.65.237'
# REDISPORT='6379'

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
    print("{} esquery() for {}-{},{} starting\nquery: {}"
          .format(datetime.utcnow(), tstart, tstop,
                  signature['name'], jsonquery))

    # form inputs to http POST call
    # headers = {'content-type': 'application/json'}
    # password stuff here as part of headers?
    # posturl = "http://" + appconfig['elasticsearch_host'] + ':' + appconfig['elasticsearch_port'] + '/_search'

    # use the api
    # response = post(posturl, data=dumps(jsonquery), headers=headers)
    response = post_es(appconfig, jsonquery)
    entries = format_esquery_output(response, signature)
    # http response code was not 200, and was thus an error
    if len(entries) == 1:
        if entries[0].keys() == 'error':
            return

    lock.acquire()
    for entry in sorted(entries, key=lambda k: k['log']):
        '''
        removed 7/5/2018
        dataentry = {'signature': signature['name'], "tstart": tstart.strftime("%Y-%m-%dT%H:%M"),
                     "tstop": tstop.strftime("%Y-%m-%dT%H:%M")}
        if 'total' in entry.keys():
            dataentry['type'] = 'total'
            dataentry['value'] = entry['total']
        elif 'error' not in entry.keys():
            dataentry['type'] = 'log'
            dataentry['value'] = entry['log']
        else:
            #log
            print("{} esquery() for {}-{},{} error\n: {}".format(datetime.utcnow(), tstart, tstop, entry, jsonquery))
        print("{} esquery() test {}".format(datetime.utcnow(), dataentry))
        if 'error' in entry.keys():
            continue
        '''
        dataentry = entry.copy()
        dataentry['signature'] = signature['name']
        dataentry['tstart'] = tstart.strftime("%Y-%m-%dT%H:%M:%SZ")
        dataentry['tstop'] = tstop.strftime("%Y-%m-%dT%H:%M:%SZ")
        q.put(dataentry)
        print("{} esquery() test\n{}".format(datetime.utcnow(), dumps(dataentry)))
    lock.release()
    # log?
    print("{} esquery() for {}-{},{} completed".format(datetime.utcnow(), tstart, tstop,
                                                       signature['name']))
    return


def post_es(appconfig, jsonquery):
    # form inputs to http POST call
    headers = {'content-type': 'application/json'}
    # password stuff here as part of headers?
    if 'elasticsearch_host' not in appconfig.keys():
        return {}
    posturl = "http://" + appconfig['elasticsearch_host']
    if 'elasticsearch_port' in appconfig.keys():
        if appconfig['elasticsearch_port'] != '':
            posturl += ':' + appconfig['elasticsearch_port']
    posturl += '/_search'

    # print(posturl)

    if 'elasticsearch_user' in appconfig.keys() and 'elasticsearch_port' in appconfig.keys():
        if appconfig['elasticsearch_user'] != '' and appconfig['elasticsearch_pass'] != '':
            httpauth = auth.HTTPBasicAuth(appconfig['elasticsearch_user'], appconfig['elasticsearch_pass'])
            return post(posturl, auth=httpauth, headers=headers, data=dumps(jsonquery), timeout=1)
    return post(posturl, headers=headers, data=dumps(jsonquery))


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
    print("{} format_esquery_output() {}".format(datetime.utcnow(), response.status_code))
    if response.status_code != 200:
        print("{} format_esquery_output() {}".format(datetime.utcnow(), response.text))
        return entries
    # print("format_esquery_output()", type(response.text), response.text)
    try:
        responsedict = loads(response.text)
    except Exception:
        return [{'error':  response.text}]

    # total = responsedict['hits']['total']
    # entries.append({'total': total})
    '''
    {'tstart': '2018-06-21T16:46', 'log': 'I0621 16:46:48.800090       1
wrap.go:42] GET /api/v1/namespaces/openstack/pods?labelSelector=
application%3Dneutron%2Ccomponent%3Dneutron-ovs-agent%2Crelease_group%3Dnewton-neutron:
(34.240555ms) 200 [[kubernetes-entrypoint/v0.0.0 (linux/amd64)
kubernetes/$Format] 135.25.65.236:59846]\n', 'tstop': '2018-06-21T16:47',
'sysdata': {'sys_container': 'apiserver',
'sys_pod': 'kubernetes-apiserver-mtn21rsv005',
'sys_namespace': 'kube-system', 'sys_host': 'mtn21rsv005'},
'signature': 'sig_001',
'metadata': {'pod': '', 'namespace': '', 'latency': '34.240555m'}
    '''

    for record in responsedict['hits']['hits']:
        print(dumps(record, indent=2))
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
            entry = {'log': log, 'sysdata': sysdata, 'metadata': metadata}
            entries.append(entry)

        elif 'message' in record['_source'].keys():
            log = record['_source']['message']
            for key in sysdata_keys['message'].keys():
                try:
                    # for syslog, fluentd/bit adds metadata directly to _source
                    sysdata[key] = record['_source'][sysdata_keys['message'][key]]
                except Exception:
                    pass
            metadata = _esquery_regex(log, signature)
            entry = {'log': log, 'sysdata': sysdata, 'metadata': metadata}

    return entries


def _esquery_regex(log, signature):
    metadata = {}
    if 'regex' in signature.keys():
        for key in signature['regex'].keys():
            try:
                temp = re.match(signature['regex'][key], log)
                # temp = temp.groups()
                metadata[key] = temp.groups()[0]
            except Exception:
                metadata[key] = ''

    return metadata


def _esquery_add_timestamp(jsonquery, tstart, tstop):
    '''
    adding a time
    :param jsonquery:
    :param tstart:
    :param tstop: datetime.datetime object specifying end of window
    :return:
    '''
    # {"query": {"bool": {"must": [{"range": {"@timestamp": {"gte": starttime, "lt": endtime}}}
    # starttime = "2018-03-06T21:18:48.00"
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
        '''
        if 'correlated' in signature['type']:
            jsonquery['size'] = 10000
        else:
            jsonquery['size'] = 0
        '''
        jsonquery['size'] = 10000
    except Exception:
        jsonquery['size'] = 0
    return


def populate_query_q(q, lock, signatures, period, OFFSET=0, currenttime=datetime.utcnow()):
    '''
    populates queries with current time to next time window
    :param q:
    :param lock:
    :param tstart:
    :param period: periodicity
    :param signatures:
    :return:
    '''
    tstart = util.getstarttime(currenttime, period, OFFSET)
    tstop = util.getstoptime(currenttime, period, OFFSET)
    print("{} populate_query_q() populating queries for {} to {}".format(datetime.utcnow(), tstart, tstop))
    for signature in signatures:
        entry = [signature, tstart, tstop]
        print("{} populate_query_q() added query for signature {}, {}-{}".format(datetime.utcnow(), signature['name'],
                                                                                 tstart, tstop))
        lock.acquire()
        q.put(entry)
        lock.release()
    util.waitfromstart(datetime.utcnow(), period, delay=period-5)

    populate_query_q(q, lock, signatures, period, OFFSET, currenttime=datetime.utcnow())


def manage_esquery(appconfig, queryq, dataq, lock):
    # arguments for esquery(): signature, tstart, tstop, appconfig, q, lock
    queryproclist = []
    proclist = []
    starttime = util.waitfromstart(datetime.utcnow(), appconfig['period'])
    while 1:
        print(datetime.utcnow(), "manage_esquery(), process list kick off and population")
        lock.acquire()
        while not queryq.empty():
            queryproclist.append(queryq.get())
        lock.release()

        while 1:
            for i in reversed(range(len(proclist))):
                if not proclist[i].is_alive():
                    proclist.pop(i)
                    # print("process pop", proclist)
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

        print(datetime.utcnow(), "manage_esquery(), all processes kicked off, awaiting next set")
        starttime = util.waitfromstart(starttime, appconfig['period'])
        # print(proclist)


if __name__ == '__main__':
    # main function and runner
    '''
    try:
        if len(argv)<2:
            envtype = getenv("ENVIRONMENT")
        else:
            envtype = argv[1]
        appconfigfile = cfg.appconfigfile[envtype]
    except KeyError:
        # logging?
        print("appconfigfile has not been selected or does not exist")
        raise
    except:
        print("unknown error loading application configuration")
        exit(0)
    '''
    if len(argv) < 2:
        appconfigfile = cfg.getconfigfile()
    else:
        appconfigfile = cfg.getconfigfile(argv[1])

    appconfig = cfg.load_config(appconfigfile)
    print(appconfig)
    signatures = cfg.load_signatures_siglist(appconfig['siglistfile'])
    if 'regexfile' in appconfig.keys():
        signatures = cfg.load_regex(signatures, appconfig['regexfile'])

    '''
    queries = []
    pop_q_process = Process(target=populate_query_q,
                            args=[QUERY_Q, LOCK, signatures, appconfig['period'],
                            appconfig['offset']])
    pop_q_process.start()


    #arguments for manage_esquery (appconfig, queryq, dataq, lock)
    manage_esquery_process = Process(target=manage_esquery,
                                     args=[appconfig, QUERY_Q, DATA_Q, LOCK])
    manage_esquery_process.start()
    # manage_esquery_process.join()

    #publish_process = Process(target=chomp_redis.chomp_redis_pub,
    #                          args = [env.redishost, env.redisport, DATA_Q, LOCK, appconfig['period'],
    #                                  10])
    '''

    datafile = './testdata/data_collector_output_sample_new2'
    query_process = Process(target=simdata.populate_q,
                            args=[datafile, appconfig['period'], appconfig['offset'],
                                  0, DATA_Q, LOCK])

    query_process.start()

    publish_process = Process(target=chomp_redis.chomp_redis_pub,
                              args=[appconfig['redishost'], appconfig['redisport'],
                                    DATA_Q, LOCK, appconfig['period'],
                                    appconfig["data_pub_delay"]])
    publish_process.start()
    publish_process.join()

    '''
    starttime = util.waitfromstart(datetime.utcnow(), appconfig['period'])
    while 1:
        starttime = util.waitfromstart(starttime, appconfig['period'])
        if DATA_Q.empty():
            continue
        LOCK.acquire()
        while not DATA_Q.empty():
            print(DATA_Q.get())
        LOCK.release()
        # print(datetime.utcnow(), "test test test")
    '''

    '''
    starttime = util.waitfromstart(datetime.utcnow(), appconfig['period'])
    while 1:
        LOCK.acquire()
        while not QUERY_Q.empty():
            queries.append(QUERY_Q.get())
        LOCK.release()
        print(datetime.utcnow(), "length of queries:", len(queries))
        for query in queries:
            print(query)
        starttime = util.waitfromstart(starttime, appconfig['period'])
    '''
