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
import chomp_util as util
import chomp_redis
import chomp_config_load as cfg
from multiprocessing import Process, Queue, Lock
from datetime import datetime
import json
from time import sleep
import redis
from copy import deepcopy
from akrainoLogger import akraino_logger


def frameInfo(frame):
    return os.path.basename(frame.f_code.co_filename) + \
        "|" + str(frame.f_lineno) + "|" + frame.f_code.co_name


def getDataFromQ(logs, DataQ, LOCK):

    ''' retrieve logs from DataQ and push into logs structure
    logs struture:
    logs['signature_name'] = [log1, log2, log3, ...]
    logn = {name: 'signature_name', 'log':KUBERNETESLOG, 'tstart': start tstamp, 'tstop': stop tstamp, ...}
    '''
    logstore = []
    # pull logs out of DataQ quickly to keep from locking too long
    LOCK.acquire()
    while not DataQ.empty():
        logstore.append(DataQ.get())
    LOCK.release()

    # put all logs from dataQ  into the logs dictionary passed into the function
    for log in logstore:
        try:
            if log['signature'] not in logs:
                logs[log['signature']] = []
            logs[log['signature']].append(log)
        except Exception:
            pass
    return


def clearLogs(logs, tcutoff=None):

    logger.info("clearLogs() removing all logs before {}"
                .format(tcutoff),
                frameInfo(sys._getframe()))

    for key in logs.keys():
        while 1:
            if len(logs[key]) == 0:
                break
            if logs[key][0]['tstart'] < tcutoff:
                logs[key].pop(0)
            else:
                break
    return


def load_corr_signatures(corrsiglistfile, basedir=os.getenv("CONFIG_DIR")):

    if basedir is None:
        basedir = '.'
    fqcorrsiglistfile = basedir + '/' + corrsiglistfile

    ''' load correlated log signatures
    '''
    required_keys = ['name', 'sig_list', 'time_limit']
    returnsig = []
    siglist = []
    with open(fqcorrsiglistfile) as f:
        for filename in f:
            fqfilename = filename.lstrip().rstrip()
            if fqfilename[0] == '#':
                continue
            fqfilename = basedir + '/' + fqfilename
            with open(fqfilename) as g:
                logger.info('load_corr_signature() opening {}'
                            .format(fqfilename),
                            frameInfo(sys._getframe()))
                try:
                    config = json.load(g)
                    for key in required_keys:
                        config[key]
                    returnsig.append(config)
                except KeyError:
                    logger.info("load_corr_signature() correlated log sig file \"{}\" doesn't have all req keys"
                                .format(fqfilename),
                                frameInfo(sys._getframe()))
                except Exception:
                    logger.info("load_corr_signature() correlated log sig file \"{}\" is malformed"
                                .format(fqfilename),
                                frameInfo(sys._getframe()))
                    continue
            returnsig[-1]['sigs_needed'] = []
            for sig in returnsig[-1]['sig_list']:
                temp = sig.lstrip().rstrip()
                if temp.lower()[:3] == 'not':
                    temp = temp[4:]
                if temp not in siglist:
                    siglist.append(temp)
                if temp not in returnsig[-1]['sigs_needed']:
                    returnsig[-1]['sigs_needed'].append(temp)

    return returnsig, siglist

# def corrlog_counter_test(signature, data, PubQ):


def corrlog_counter_test(signature, logdata, tstart_window, tstop_window, PubQ=None, Lock=None):

    ''' this is the main fucntion that does correlation and counting, work on this first
    '''
    # make sure there is a check that the final event in the correlation is
    # in the current timeframe (bleedover)
    # time window limit is in signature
    logger.info("corrlog_counter_test() beginning processing for timewindow {}-{} counter \'{}\'"
                .format(tstart_window, tstop_window, signature['name']),
                frameInfo(sys._getframe()))
    logger.info("corrlog_counter_test() for counter {}, signatures needed: {}"
                .format(signature['name'], signature['sigs_needed']),
                frameInfo(sys._getframe()))
    logger.info("corrlog_counter_test() for counter {}, signatures passed: {}"
                .format(signature['name'], logdata.keys()),
                frameInfo(sys._getframe()))
    sleep(5)

    count = {}
    for sig in logdata.keys():
        count[sig] = {}
        for log in logdata[sig]:
            timekey = log['tstart']+'-'+log['tstop']
            if timekey not in count[sig].keys():
                count[sig][timekey] = 0
            count[sig][timekey] += 1

    for sig in count.keys():
        for tstamp in count[sig].keys():
            logger.info(tstart+","+tstop+","+sig+","+tstamp+","+count[sig][tstamp],
                        frameInfo(sys._getframe()))
    logger.info("corrlog_counter_test() completed processing for counter \'{}\'"
                .format(signature['name']),
                frameInfo(sys._getframe()))
    doc = {'sig': signature['name'], 'count': count, 'tstart': tstart_window, 'tstop': tstop_window}

    if PubQ:
        Lock.acquire()
        PubQ.put(doc)
        Lock.release()

    return


def corrlog_counter(signature, logdata, tstart_window, tstop_window, PubQ=None, Lock=None):
    ''' this is the main fucntion that does correlation and counting, work on this first
    '''
    # make sure there is a check that the final event in the correlation is in the current timeframe (bleedover)
    # time window limit is in signature
    outputdoc = {}
    # debug 201811
    # data = logdata.copy()
    data = deepcopy(logdata)
    try:
        outputdoc['name'] = signature['name']
    except Exception:
        logger.info("corrlog_counter() corrlog signature document malformed",
                    frameInfo(sys._getframe()))
        return

    outputdoc['tstart'] = tstart_window
    outputdoc['tstop'] = tstop_window
    outputdoc['type'] = "correlated"

    tstartdate_window, tstartsec_window = convert_iso8601_tstamp(tstart_window)
    tstopdate_window, tstopsec_window = convert_iso8601_tstamp(tstop_window)

    if tstartsec_window > tstopsec_window:
        tstopsec_window += 86400

    # siglist processing
    try:
        siglist = signature['sig_list'].copy()
    except Exception:
        logger.info("corrlog_counter() malformed signature file ",
                    frameInfo(sys._getframe()))

    notsiglist = []
    index = 0
    while True:
        # for i in range(len(siglist)):
        if 'not' in siglist[index]:
            sig = siglist[index].lstrip().rstrip().split(' ')[-1]
            notsiglist.append((sig, index))
            siglist.pop(index)
        else:
            index += 1

        if index >= len(siglist):
            break

    firstsig = siglist[0]
    if 'match' in signature.keys():
        match = signature['match']
    else:
        match = None

    eventcount = 0
    latency = []

    # added with new, 2018/11/07
    '''
    if 'keep_succ_logs' in signature.keys():
        if signature['keep_succ_logs']:
            outputdoc['success_logs'] = []
    else:
        pass

    if 'keep_fail_logs' in signature.keys():
        if signature['keep_fail_logs']:
            outputdoc['failure_logs'] = []
    else:
        pass
    '''
    outputdoc['logs'] = []

    while 1:
        # outer loop to advance first sig in the sig series
        # keeps track of events in the series
        events = []
        eventtimes = []

        # keeps track of indexes in the series
        indexlist = []  # keeps track of indexes in each of the other events

        # takes first event
        try:
            # TODO: ADD CODE HERE TO REMOVE ANY firstsig EVENT BEFORE TSTART-TIMELIMIT
            events.append(data[firstsig][0])
        except KeyError:
            logger.info("corrlog_counter() start signature \"{}\" does not exist"
                        .format(firstsig),
                        frameInfo(sys._getframe()))
            break
        except IndexError:
            logger.info("corrlog_counter() out of indices for \"{}\""
                        .format(firstsig),
                        frameInfo(sys._getframe()))
            break
        except Exception as exc:
            logger.info("corrlog_counter() unknown error occurred for \"{}\", err={}"
                        .format(firstsig, str(exc)),
                        frameInfo(sys._getframe()))
            break

        indexlist.append(0)
        startdate, starttstamp = convert_log_tstamp(events[0]['log'])

        prevtstamp = starttstamp
        timestamp = starttstamp
        data[firstsig].pop(0)
        eventtimes.append(timestamp)

        matchval = ''
        if match:
            try:
                matchval = events[0]['metadata'][match]
                if matchval == '':
                    logger.info("corrlog_counter() no matchval found for \"{}\""
                                .format(match),
                                frameInfo(sys._getframe()))
                    logger.info(events[0], frameInfo(sys._getframe()))
                    continue
            except Exception:
                continue

        for i in range(1, len(siglist)):
            sig = siglist[i]
            index = 0
            while 1:
                try:
                    log = data[sig][index]
                except Exception:
                    # if sig doesnt exist in results OR index out of range, then you're done looking
                    events.append("")
                    indexlist.append(-1)
                    break
                date, timestamp = convert_log_tstamp(log['log'])

                if date != startdate:
                    timestamp += 86400

                tdiff = timestamp - starttstamp

                if tdiff <= 0:
                    # since all results reported in order, you can pop the log because it will definitely be
                    # before the next start event
                    data[sig].pop(index)
                    continue
                # check if timestamp is within range, if not, stop looking
                elif tdiff > signature['time_limit']:
                    events.append("")
                    indexlist.append(-1)
                    break
                elif timestamp < prevtstamp:
                    index += 1
                    continue

                if match:
                    try:
                        # if metadata for match exists but isn't the same
                        if log['metadata'][match] != matchval:
                            index += 1
                            continue
                    except Exception:
                        # if the metadata for match didn't exist at all
                        index += 1
                        continue

                # if you've made it here, you have a match - we've now checked
                # 1) if tstamp within range of starttstamp
                # 2) if this tstamp is after the previous signature timestamp
                # 3) if there is a matching criteria
                # 4) if 3), if the current event matching criteria (e.g. pod) matches start event

                indexlist.append(index)
                events.append(log)
                eventtimes.append(timestamp)
                data[sig].pop(index)
                break

            if indexlist[-1] == -1:
                # did not find a match for most recent positive sig, no counting can occur here
                # for event in events:
                #     logger.info(event, frameInfo(sys._getframe()))
                # logger.info(indexlist, frameInfo(sys._getframe()))

                # added with new, 2018/11/07
                '''
                if 'failure_logs' in outputdoc.keys():
                    outputdoc['failure_logs'].append(_format_events(events))
                '''

                break

            # update "prevtstamp", only occurs if previous signature was a match
            prevtstamp = timestamp

        # cycled through all of the logs except the "not" logs
        if len(indexlist) == len(siglist) and indexlist[-1] != -1:
            # found match for all sigs for the correlated log event
            # this is all just checking, will comment out later
            ''' debug 201811 remove log
            logger.info("corrlog_counter() full signature found for kpi {} matchval \"{}\""
                        .format(signature['name'], matchval),
                        frameInfo(sys._getframe()))
            '''

            # do a notsig check
            notsigevent, notsigindex = _check_not_sig(notsiglist, eventtimes, match, matchval,
                                                      signature['time_limit'], data)

            # do a timestamp check
            rangeflag = _check_final_timestamp(eventtimes[-1], tstartsec_window, tstopsec_window)

            if not rangeflag:
                pass
            elif notsigindex != '':
                logger.info("corrlog_counter() full signature negated for kpi {} matchval \"{}\""
                            .format(signature['name'], matchval),
                            frameInfo(sys._getframe()))

                # added with new, 2018/11/07
                '''
                if 'failure_logs' in outputdoc.keys():
                    if notsigindex != '' and rangeflag:
                        events.insert(notsigindex, notsigevent)
                        outputdoc['failure_logs'].append(_format_events(events))
                '''
            else:
                eventcount += 1
                latency.append(tdiff)
                '''
                if 'success_logs' in outputdoc.keys():
                    outputdoc['success_logs'].append(_format_events(events))
                '''
                # added with new, 2018/11/07
                if 'logs' in outputdoc.keys():
                    outputdoc['logs'].append(_format_events(events))

    outputdoc['count'] = eventcount
    outputdoc['latency'] = latency

    # debug 201811
    logger.info("logs copied for sig {},{}-{}"
                .format(signature['name'], tstart_window, tstop_window),
                frameInfo(sys._getframe()))

    # added with new, 2018/11/07
    try:
        outputdoc['latency_max'] = max(latency)
        outputdoc['latency_min'] = min(latency)
        outputdoc['latency_avg'] = sum(latency)/len(latency)
    except Exception:
        outputdoc['latency_max'] = None
        outputdoc['latency_min'] = None
        outputdoc['latency_avg'] = None

    # debug 201811
    logger.info("got latency values for sig {}, {}-{}"
                .format(signature['name'], tstart_window, tstop_window),
                frameInfo(sys._getframe()))
    # add data to queue to publish out
    if PubQ:
        Lock.acquire()
        PubQ.put(outputdoc)
        Lock.release()

    logger.info("pushed values to PubQ for sig {}, {}-{}"
                .format(signature['name'], tstart_window, tstop_window),
                frameInfo(sys._getframe()))
    return


def _check_not_sig(notsiglist, eventtimes, match, matchval, time_limit, data):

    ''' this checks if there are "NOT" signatures that negate correlated log signatures
    '''
    notsigflag = False
    returnevent = ''
    returnindex = ''

    for i in range(len(notsiglist)):
        sig = notsiglist[i][0]
        targetindex = notsiglist[i][1]
        index = 0
        while 1:
            try:
                log = data[sig][index]
            except Exception:
                break
            if match:
                try:
                    matchval_notsig = log['metadata'][match]
                    if matchval != matchval_notsig:
                        index += 1
                        continue
                except Exception:
                    index += 1
                    continue

            # just a check, remove
            logger.info("_check_not_sig() log found for matchval \"{}\""
                        .format(matchval),
                        frameInfo(sys._getframe()))
            logger.info(log, frameInfo(sys._getframe()))

            date, tstamp = convert_log_tstamp(log['log'])
            if targetindex > 0:
                if tstamp < eventtimes[targetindex] and tstamp > eventtimes[targetindex-1]:
                    notsigflag = True
                    returnindex = targetindex
                    returnevent = log
                    data[sig].pop(index)
                    break
            else:
                if tstamp < eventtimes[targetindex] and tstamp > (eventtimes[targetindex]-time_limit):
                    notsigflag = True
                    returnindex = targetindex
                    returnevent = log
                    data[sig].pop(index)
                    break

            index += 1

        if notsigflag:
            break

    # return notsigflag
    return returnevent, returnindex


def _check_final_timestamp(eventtime, tstart, tstop):
    flag = False
    time = eventtime
    if time > 86400:
        time -= 86400
    if time >= tstart and time < tstop:
        flag = True
    return flag


def _format_events(events):
    eventoutput = []
    for event in events:
        try:
            temp = {}
            temp['log'] = event['log']
            temp['signature'] = event['signature']
            temp['tstart'] = event['tstart']
            temp['tstop'] = event['tstop']
            temp['metadata'] = event['metadata'].copy()
            temp['sysdata'] = event['sysdata'].copy()
            eventoutput.append(temp)
        except Exception:
            pass
    return eventoutput


def convert_log_tstamp(log, inseconds=True):
    date = log[1:5]
    time = log[6:20]
    # do a conversion to seconds
    try:
        if inseconds:
            tok = time.split(':')
            time = 3600 * int(tok[0]) + 60 * int(tok[1]) + float(tok[2])
    except Exception:
        logger.info("bad time: \"{}\""
                    .format(log[6:20]),
                    frameInfo(sys._getframe()))
        time = 0

    return date, time


def convert_iso8601_tstamp(tstamp, inseconds=True):
    date = None
    time = None
    try:
        time = datetime.strptime(tstamp, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        logger.info("timestamp {} not formed as iso-8601"
                    .format(tstamp),
                    frameInfo(sys._getframe()))
        return date, time

    date = tstamp[0:10]
    time = tstamp[11:19]
    if inseconds:
        tok = time.split(':')
        time = 3600 * int(tok[0]) + 60 * int(tok[1]) + float(tok[2])
    return date, time


def manage_counter_processes(signatures, logdata, tstart, tstop, PubQ=None, Lock=None):

    '''
    this will manage the number of corrlog processes
    '''
    proclist = []
    # debug 201811
    debuglist = []

    logger.info("manage_counter_processes() process list kick off and population", frameInfo(sys._getframe()))
    index = 0
    while 1:
        if index == len(signatures):
            logger.info("manage_counter_processes() waiting for last processes of {}-{} to complete"
                        .format(tstart, tstop), frameInfo(sys._getframe()))
            while 1:
                for i in reversed(range(len(proclist))):
                    if not proclist[i].is_alive():
                        proclist.pop(i)
                        # debug 201811
                        debuglist.pop(i)
                        logger.info("manage_counter_processes() popping process last index {}-{}"
                                    .format(tstart, tstop),
                                    frameInfo(sys._getframe()))
                        logger.info('remaining processes:'+str(debuglist),
                                    frameInfo(sys._getframe()))
                if len(proclist) == 0:
                    break
                sleep(.1)
            break

        # clear out finished proceses
        for i in reversed(range(len(proclist))):
            if not proclist[i].is_alive():
                proclist.pop(i)
                # debug 201811
                debuglist.pop(i)
                logger.info("manage_counter_processes() popping process not last index {}-{}"
                            .format(tstart, tstop),
                            frameInfo(sys._getframe()))
                logger.info('remaining processes:'+str(debuglist),
                            frameInfo(sys._getframe()))

        # add processes up to process count limit
        for i in range(appconfig['corr_proc_count'] - len(proclist)):
            tempdata = {}
            for sig in signatures[index]['sigs_needed']:
                try:
                    # debug 201811
                    tempdata[sig] = logdata[sig].copy()
                    # tempdata[sig] = deepcopy(logdata[sig])
                except Exception:
                    pass
            proclist.append(Process(target=corrlog_counter,
                            args=[signatures[index], tempdata, tstart, tstop, PubQ, Lock]))
            # debug 201811
            debuglist.append("corrlog_count process {} {} {} at time {}"
                             .format(signatures[index]['name'], tstart, tstop, datetime.utcnow()))
            proclist[-1].start()
            index += 1
            if index == len(signatures):
                break
            sleep(.1)

        if len(proclist) == appconfig['corr_proc_count']:
            sleep(.1)
            continue

    # debug 201811 logger.info("manage_counter_processes() process list kick off and population",
    #           frameInfo(sys._getframe()))
    logger.info("*************************************************************************",
                frameInfo(sys._getframe()))
    logger.info("*************************************************************************",
                frameInfo(sys._getframe()))
    logger.info("manage_counter_processes() process list completed for {}-{}"
                .format(tstart, tstop),
                frameInfo(sys._getframe()))
    logger.info("*************************************************************************",
                frameInfo(sys._getframe()))
    logger.info("*************************************************************************",
                frameInfo(sys._getframe()))
    return


if __name__ == '__main__':
    # main function and runner

    func = 'corrlog_count'

    logger = akraino_logger()
    logger.info(("%s: entry" % (func)), frameInfo(sys._getframe()))

    DataQ = Queue()
    PubQ = Queue()
    LOCK = Lock()

    if len(sys.argv) < 2:
        appconfigfile = cfg.getconfigfile(func)
    else:
        appconfigfile = cfg.getconfigfile(sys.argv[1])

    appconfig = cfg.load_config(appconfigfile)
    corrsigs, subsignaturelist = load_corr_signatures(appconfig['corrsiglistfile'])
    logger.info("chomp_corrlog_count() data signatures needed: {}"
                .format(subsignaturelist),
                frameInfo(sys._getframe()))
    logger.info("chomp_corrlog_count() correlated log signatures: {} "
                .format(corrsigs),
                frameInfo(sys._getframe()))

    lookback = 0
    for sig in corrsigs:
        sig['lookback'] = (sig['time_limit']//appconfig['period'])*appconfig['period'] \
                          + ((sig['time_limit'] % appconfig['period']) > 0)*appconfig['period']
        sig['lookback'] = -sig['lookback']
        if sig['lookback'] < lookback:
            lookback = sig['lookback']
    logger.info("chomp_corrlog_counter() list of signatures:", frameInfo(sys._getframe()))
    for sig in corrsigs:
        logger.info(sig, frameInfo(sys._getframe()))

    logger.info("chomp_corrlog_count() data will have lookback of {}s"
                .format(lookback),
                frameInfo(sys._getframe()))

    # set up redis publisher
    pubtopic = appconfig['redis_pub_channel']
    redis_pub = redis.Redis(host=appconfig['redis_host'],
                            port=appconfig['redis_port'],
                            db=appconfig['redis_db'])

    subscribe_process = Process(target=chomp_redis.chomp_redis_sub,
                                args=[appconfig['redis_host'],
                                      appconfig['redis_port'], subsignaturelist,
                                      appconfig['period'],
                                      DataQ, LOCK])
    subscribe_process.start()

    logger.info("chomp_corrlog_count() start processing data", frameInfo(sys._getframe()))
    util.waitfromstart(datetime.utcnow(), appconfig['period'], delay=appconfig['data_read_delay'])

    initial_offset = appconfig['offset']+appconfig['period']
    if appconfig['data_pub_delay'] >= appconfig['data_sub_delay']:
        initial_offset += appconfig['period']
        if appconfig['data_sub_delay'] >= appconfig['data_read_delay']:
            initial_offset += appconfig['period']
    else:
        if appconfig['data_sub_delay'] >= appconfig['data_read_delay']:
            initial_offset += appconfig['period']

    timeedge = util.getstarttime(datetime.utcnow(), appconfig['period'])
    tstart = util.getstarttime(timeedge, appconfig['period'], OFFSET=initial_offset)
    tstop = util.getstoptime(timeedge, appconfig['period'], OFFSET=initial_offset)

    logs = {}
    while 1:
        logger.info("chomp_corrlog_count() beginning event counting for time window {}, {}"
                    .format(tstart, tstop),
                    frameInfo(sys._getframe()))

        count = {}
        getDataFromQ(logs, DataQ, LOCK)
        for sig in logs.keys():
            count[sig] = {}
            for log in logs[sig]:
                timekey = log['tstart']+'-'+log['tstop']
                if timekey not in count[sig].keys():
                    count[sig][timekey] = 0
                count[sig][timekey] += 1

        manage_counter_processes(corrsigs, logs, tstart.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                 tstop.strftime("%Y-%m-%dT%H:%M:%SZ"), PubQ=PubQ, Lock=LOCK)

        logger.info("chomp_corrlog_count() event counting completed for time window {}, {}"
                    .format(tstart, tstop),
                    frameInfo(sys._getframe()))

        # publish data to redis
        LOCK.acquire()
        logger.info("chomp_corrlog_counter() results to publish for {} - {}"
                    .format(tstart, tstop),
                    frameInfo(sys._getframe()))
        while not PubQ.empty():
            message = PubQ.get()
            redis_pub.publish(pubtopic, json.dumps(message))
            # logger.info(json.dumps(message, indent=2), frameInfo(sys._getframe()))
        LOCK.release()

        # increment timestamp
        tstart = util.addtime(tstart, appconfig['period'])
        tstop = util.addtime(tstop, appconfig['period'])
        timeedge = util.addtime(timeedge, appconfig['period'])

        clearLogs(logs, tcutoff=util.addtime(tstart, lookback).strftime("%Y-%m-%dT%H:%M:%SZ"))

        waittime = (timeedge-datetime.utcnow()).total_seconds()+appconfig['data_read_delay']

        if waittime >= 0:
            logger.info("chomp_corrlog_count() sleeping for {}s to calculate KPIs for time window {}, {}"
                        .format(waittime, tstart, tstop),
                        frameInfo(sys._getframe()))
            sleep(waittime)
        else:
            logger.info("chomp_corrlog_count() behind schedule for calculating time window {}, {}"
                        .format(tstart, tstop),
                        frameInfo(sys._getframe()))
