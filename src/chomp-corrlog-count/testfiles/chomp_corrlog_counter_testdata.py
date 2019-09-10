import chomp_util as util
import chomp_config_load as cfg
import corrlog_test_data as simdata
from multiprocessing import Process, Queue, Lock
from datetime import datetime
from sys import argv
import json
from time import sleep
import redis


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
    # while not DataQ.empty():
    for log in logstore:
        try:
            if log['signature'] not in logs:
                logs[log['signature']] = []
            logs[log['signature']].append(log)
        except Exception:
            pass
    return


def clearLogs(logs, tcutoff=None):
    print("{} clearLogs() removing all logs before {}".format(datetime.utcnow(), tcutoff))

    for key in logs.keys():
        while 1:
            if len(logs[key]) == 0:
                break
            if logs[key][0]['tstart'] < tcutoff:
                logs[key].pop(0)
            else:
                break
    return


def load_corr_signatures(corrsiglistfile):
    ''' load correlated log signatures
    '''
    required_keys = ['name', 'sig_list', 'time_limit']
    returnsig = []
    siglist = []
    with open(corrsiglistfile) as f:
        for filename in f:
            if filename.lstrip().rstrip()[0] == '#':
                continue
            with open(filename.lstrip().rstrip()) as g:
                print('{} load_corr_signature() opening {}'.format(datetime.utcnow(), filename.rstrip()))
                try:
                    config = json.load(g)
                    for key in required_keys:
                        config[key]
                    returnsig.append(config)
                except KeyError:
                    print("{} load_corr_signature() correlated log sig file \"{}\" does not have all required keys"
                          .format(datetime.utcnow(), filename.rstrip()))
                except Exception:
                    print("{} load_corr_signature() correlated log sig file \"{}\" is malformed"
                          .format(datetime.utcnow(), filename.rstrip()))
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


def corrlog_counter_test(signature, logdata, tstart_window, tstop_window, PubQ=None, Lock=None):

    ''' this is the main fucntion that does correlation and counting, work on this first
    '''
    # make sure there is a check that the final event in the correlation is in the current timeframe (bleedover)
    # time window limit is in signature
    print("{} corrlog_counter_test() beginning processing for timewindow {}-{} counter \'{}\'"
          .format(datetime.utcnow(), tstart_window, tstop_window, signature['name']))
    print("{} corrlog_counter_test() for counter {}, signatures needed: {}"
          .format(datetime.utcnow(), signature['name'], signature['sigs_needed']))
    print("{} corrlog_counter_test() for counter {}, signatures passed: {}"
          .format(datetime.utcnow(), signature['name'], logdata.keys()))
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
            print(datetime.utcnow(), tstart, tstop, sig, tstamp, count[sig][tstamp])
    print("{} corrlog_counter_test() completed processing for counter \'{}\'"
          .format(datetime.utcnow(), signature['name']))
    # print("\n\n",datetime.utcnow(),sig,"\n")
    # for log in logs[sig]:
    #     print(log['tstart'],log['tstop'],log['log'])

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
    data = logdata.copy()
    try:
        outputdoc['name'] = signature['name']
    except Exception:
        print("{} corrlog_counter() corrlog signature document malformed".format(datetime.utcnow()))
        return

    outputdoc['tstart'] = tstart_window
    outputdoc['tstop'] = tstop_window

    tstartdate_window, tstartsec_window = convert_iso8601_tstamp(tstart_window)
    tstopdate_window, tstopsec_window = convert_iso8601_tstamp(tstop_window)

    if tstartsec_window > tstopsec_window:
        tstopsec_window += 86400

    # siglist processing
    try:
        siglist = signature['sig_list'].copy()
    except Exception:
        print("{} corrlog_counter() malformed signature file ".format(datetime.utcnow()))

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

    # check of signature list processing
    # print(siglist)
    # print(notsiglist)
    # return

    firstsig = siglist[0]
    if 'match' in signature.keys():
        match = signature['match']
    else:
        match = None

    eventcount = 0
    latency = []

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

    while 1:
        # outer loop to advance first sig in the sig series
        # keeps track of events in the series
        events = []
        eventtimes = []

        # keeps track of indexes in the series
        indexlist = []  # keeps track of indexes in each of the other events

        # takes first event
        try:
            # ADD CODE HERE TO REMOVE ANY firstsig EVENT BEFORE TSTART-TIMELIMIT
            # print("\n\n\n\n\nMAKEEDITS CORRLOG_COUNTER STUPID\n\n\n")
            '''
            while 1:
                startdatendow, tstartsec_window , starttstamp = convert_log_tstamp(events[0]['log'])

                if startdate == tstartdate_window[5:].replace("-",""):
                    if starttstamp <  tstartsec_window

            '''
            events.append(data[firstsig][0])
        except KeyError:
            print("{} corrlog_counter() start signature \"{}\" does not exist".format(datetime.utcnow(), firstsig))
            break
        except IndexError:
            print("{} corrlog_counter() out of indices for \"{}\"".format(datetime.utcnow(), firstsig))
            break
        except Exception:
            print("{} corrlog_counter() unknown error occurred for \"{}\"".format(datetime.utcnow(), firstsig))
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
                    print("{} corrlog_counter() no matchval found for \"{}\"".format(datetime.utcnow(), match))
                    print(events[0])
                    continue
            except Exception:
                continue

        print("{} corrlog_counter() now counting for kpi {} matchval \"{}\""
              .format(datetime.utcnow(), signature['name'], matchval))
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
                print("{} corrlog_counter() signature failed for kpi {} matchval \"{}\""
                      .format(datetime.utcnow(), signature['name'], matchval))
                # for event in events:
                #     print(event)
                # print(indexlist)
                if 'failure_logs' in outputdoc.keys():
                    outputdoc['failure_logs'].append(_format_events(events))
                break

            '''
            if len(indexlist) == len(siglist):
                # found match for all sigs for the correlated log event
                # this is all just checking, will comment out later
                print("{} corrlog_counter() full signature found \"{}\"".format(datetime.utcnow(), matchval))
                for event in events:
                    print(event)
                print(indexlist)
                print("latency", tdiff)

                # run check against data
                eventcount += 1
                latency.append(tdiff)
                # no need to break, this is last of the sig list anyways
            '''
            # update "prevtstamp", only occurs if previous signature was a match
            prevtstamp = timestamp

        # cycled through all of the logs except the "not" logs
        if len(indexlist) == len(siglist) and indexlist[-1] != -1:
            # found match for all sigs for the correlated log event
            # this is all just checking, will comment out later
            print("{} corrlog_counter() full signature found for kpi {} matchval \"{}\"".format(datetime.utcnow(),
                  signature['name'], matchval))
            '''
            rangeflag = _check_final_timestamp(eventtimes[-1], tstartsec_window, tstopsec_window)

            if not rangeflag:
                print("{} corrlog_counter() full signature negated for \"{}\"".format(datetime.utcnow(), matchval))
                break

            # do a notsig check
            notsigevent, notsigindex = _check_not_sig(notsiglist, eventtimes, match, matchval,
                                                      signature['time_limit'], data)
            if notsigindex != '':
                print("{} corrlog_counter() full signature negated for \"{}\"".format(datetime.utcnow(), matchval))
                if 'failure_logs' in outputdoc.keys():
                    if notsigindex != '' and rangeflag:
                        events.insert(notsigindex, notsigevent)
                        outputdoc['failure_logs'].append(_format_events(events))
                break

            # this is a successful signature captured, incrememnt what is needed
            eventcount += 1
            latency.append(tdiff)
            if 'success_logs' in outputdoc.keys():
                outputdoc['success_logs'].append(_format_events(events))

            '''

            # do a notsig check
            notsigevent, notsigindex = _check_not_sig(notsiglist, eventtimes, match, matchval,
                                                      signature['time_limit'], data)

            # do a timestamp check
            rangeflag = _check_final_timestamp(eventtimes[-1], tstartsec_window, tstopsec_window)

            if not rangeflag:
                print("{} corrlog_counter() final timestamp is outside of time window for kpi {} matchval \"{}\""
                      .format(datetime.utcnow(), signature['name'], matchval))
            elif notsigindex != '':
                print("{} corrlog_counter() full signature negated for kpi {} matchval \"{}\""
                      .format(datetime.utcnow(), signature['name'], matchval))
                if 'failure_logs' in outputdoc.keys():
                    if notsigindex != '' and rangeflag:
                        events.insert(notsigindex, notsigevent)
                        outputdoc['failure_logs'].append(_format_events(events))
            else:
                eventcount += 1
                latency.append(tdiff)
                if 'success_logs' in outputdoc.keys():
                    outputdoc['success_logs'].append(_format_events(events))

    outputdoc['count'] = eventcount
    outputdoc['latency'] = latency

    # add data to queue to publish out
    if PubQ:
        Lock.acquire()
        PubQ.put(outputdoc)
        Lock.release()

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
            print("\n{} _check_not_sig() log found for matchval \"{}\"".format(datetime.utcnow(), matchval))
            print(log, '\n')

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
            eventoutput.append(temp)
        except Exception:
            pass
    return eventoutput


def convert_log_tstamp(log, inseconds=True):
    date = log[1:5]
    time = log[6:20]
    # do a conversion to seconds
    if inseconds:
        tok = time.split(':')
        time = 3600 * int(tok[0]) + 60 * int(tok[1]) + float(tok[2])

    return date, time


def convert_iso8601_tstamp(tstamp, inseconds=True):
    # print("here", tstamp)
    date = None
    time = None
    try:
        time = datetime.strptime(tstamp, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        print("{} convert_iso8601_tstamp() timestamp {} not formed as iso-8601".format(datetime.utcnow(), tstamp))
        return date, time

    date = tstamp[0:10]
    time = tstamp[11:19]
    # time = mktime(time.timetuple())
    # print(date+'XXXX'+time+'XXXX')
    if inseconds:
        tok = time.split(':')
        time = 3600 * int(tok[0]) + 60 * int(tok[1]) + float(tok[2])
    return date, time


def manage_counter_processes(signatures, logdata, tstart, tstop, PubQ=None, Lock=None):
    ''' this will manage the number of corrlog processes
    '''
    proclist = []

    print(datetime.utcnow(), "manage_counter_processes() process list kick off and population")
    index = 0
    while 1:
        if index == len(signatures):
            print("{} manage_counter_processes() waiting for last processes of {}-{} to complete".format(
                  datetime.utcnow(), tstart, tstop))
            while 1:
                for i in reversed(range(len(proclist))):
                    if not proclist[i].is_alive():
                        proclist.pop(i)
                if len(proclist) == 0:
                    break
                sleep(.1)
            break

        # clear out finished proceses
        for i in reversed(range(len(proclist))):
            if not proclist[i].is_alive():
                proclist.pop(i)

        # add processes up to process count limit
        for i in range(appconfig['corr_proc_count'] - len(proclist)):
            tempdata = {}
            for sig in signatures[index]['sigs_needed']:
                try:
                    tempdata[sig] = logdata[sig].copy()
                except Exception:
                    pass
            proclist.append(Process(target=corrlog_counter,
                            args=[signatures[index], tempdata, tstart, tstop, PubQ, Lock]))
            proclist[-1].start()
            index += 1
            if index == len(signatures):
                break
            sleep(.1)

        if len(proclist) == appconfig['corr_proc_count']:
            sleep(.1)
            continue

    print(datetime.utcnow(), "manage_counter_processes() process list kick off and population")
    return


if __name__ == '__main__':
    # main function and runner

    DataQ = Queue()
    PubQ = Queue()
    LOCK = Lock()

    if len(argv) < 2:
        appconfigfile = cfg.getconfigfile()
    else:
        appconfigfile = cfg.getconfigfile(argv[1])

    appconfig = cfg.load_config(appconfigfile)
    # signatures = cfg.load_signatures_siglist(appconfig['siglistfile'])
    # subsignaturelist = chomp_redis.get_signature_list(signatures, 'correlated')
    corrsigs, subsignaturelist = load_corr_signatures(appconfig['corrsiglistfile'])
    print("{} chomp_corrlog_count() data signatures needed: {}".format(datetime.utcnow(), subsignaturelist))
    print("{} chomp_corrlog_count() correlated log signatures: ".format(datetime.utcnow()))

    lookback = 0
    for sig in corrsigs:
        sig['lookback'] = (sig['time_limit']//appconfig['period'])*appconfig['period'] \
                          + ((sig['time_limit'] % appconfig['period']) > 0)*appconfig['period']
        # if sig['time_limit']>lookback:
        #     lookback = sig['time_limit']
        if sig['lookback'] > lookback:
            lookback = sig['lookback']
    lookback = -lookback
    for sig in corrsigs:
        print(sig)
    print("{} chomp_corrlog_count() data will have lookback of {}s".format(datetime.utcnow(), lookback))

    # set up redis publisher
    pubtopic = appconfig['redis_corrlog_topic']
    redis_pub = redis.Redis(host=appconfig['redishost'], port=appconfig['redisport'], db=0)

    # print(subsignaturelist)

    '''
    subscribe_process = Process(target=chomp_redis.chomp_redis_sub,
                                args=[appconfig['redishost'], appconfig['redisport'], subsignaturelist,
                                      appconfig['period'], DataQ, LOCK])
    subscribe_process.start()
    '''

    datafile = './testdata/data_collector_output_sample_new2'
    subscribe_process = Process(target=simdata.populate_q,
                                args=[datafile, appconfig['period'], appconfig['offset'],
                                      60, DataQ, LOCK])

    subscribe_process.start()

    print("start", datetime.utcnow())
    util.waitfromstart(datetime.utcnow(), appconfig['period'], delay=appconfig['data_read_delay'])
    # print("stop",datetime.utcnow())

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
        print("{} chomp_corrlog_count() beginning event counting for time window {}, {}".format(datetime.utcnow(),
              tstart, tstop))

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

        print("{} chomp_corrlog_count() event counting completed for time window {}, {}".format(datetime.utcnow(),
              tstart, tstop))

        # publish data to redis
        LOCK.acquire()
        print("{} chomp_corrlog_counter() results to publish for {} - {}".format(datetime.utcnow(), tstart, tstop))
        while not PubQ.empty():
            message = PubQ.get()
            redis_pub.publish(pubtopic, json.dumps(message))
            # print(json.dumps(PubQ.get(), indent=2))
            print(json.dumps(message, indent=2))
        LOCK.release()

        tstart = util.addtime(tstart, appconfig['period'])
        tstop = util.addtime(tstop, appconfig['period'])
        # timeedge = util.addtime(timeedge, appconfig['period']+appconfig['data_read_delay'])
        timeedge = util.addtime(timeedge, appconfig['period'])
        clearLogs(logs, tcutoff=util.addtime(tstart, lookback).strftime("%Y-%m-%dT%H:%M:%SZ"))

        waittime = (timeedge-datetime.utcnow()).total_seconds()+appconfig['data_read_delay']

        if waittime >= 0:
            print("{} chomp_corrlog_count() sleeping for {}s to calculate KPIs for time window {}, {}"
                  .format(datetime.utcnow(), waittime, tstart, tstop))
            sleep(waittime)
        else:
            print("{} chomp_corrlog_count() behind schedule for calculating time window {}, {}"
                  .format(datetime.utcnow(), tstart, tstop))
