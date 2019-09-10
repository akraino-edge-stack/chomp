import json
from datetime import datetime
from time import sleep


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


def getstoptime(starttime, period, OFFSET=0):
    '''
    # gives next clock edge from starttime given a period
    # e.g. 5 min periodicity - will give closest 5 minute clock edge, 12:00, 12:05, 12:10...
    :return:
    '''
    return datetime.utcfromtimestamp((((starttime - datetime(1970, 1, 1)).total_seconds() - OFFSET)
                                      // period + 1) * period)


def populate_q(file, period, offset=0, delay=0, Q=None, Lock=None):
    data = []
    with open(file) as f:
        for line in f:
            if line.lstrip().rstrip() != '':
                data.append(json.loads(line.lstrip().rstrip()))

    while 1:
        newdata = []
        starttime = datetime.utcfromtimestamp((((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() - offset-delay)//period) * period)  # noqa: E501
        stoptime = datetime.utcfromtimestamp((((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() - offset-delay)//period + 1) * period)  # noqa: E501

        for message in sorted(data, key=lambda k: k['log']):
            newdata.append(message.copy())
            newdata[-1]['tstop'] = stoptime.strftime("%Y-%m-%dT%H:%M:%SZ")
            newdata[-1]['tstart'] = starttime.strftime("%Y-%m-%dT%H:%M:%SZ")
            newdata[-1]['log'] = newdata[-1]['log'].replace("IMMDD HH:mm", starttime.strftime("I%m%d %H:%M"))
            newdata[-1]['log'] = newdata[-1]['log'].replace("IMMDD HH:nn", stoptime.strftime("I%m%d %H:%M"))
            # print(message['log'])

        waitfromstart(datetime.utcnow(), period, delay)
        # starttime = datetime.utcfromtimestamp((((datetime.utcnow() -
        #    datetime(1970, 1, 1)).total_seconds() - offset -20)// period ) * period)
        # stoptime = datetime.utcfromtimestamp((((datetime.utcnow() -
        #    datetime(1970, 1, 1)).total_seconds() - offset-20)// period +1) * period)
        print(datetime.utcnow(), "populate_q()", "test data loaded for ",  starttime, stoptime)
        # for message in newdata:
        #     print(message['tstart'], message['tstop'], message['log'])
        # do a replace of all the records in data
        # _replace_time()
        if Q:
            Lock.acquire()
            for message in newdata:
                Q.put(message)
            Lock.release()


if __name__ == '__main__':

    # this has data that crosses over with data all in correct window
    # file = './testdata/data_collector_output_sample_new2'

    # has data that crosses over with some records in the "next" timewindow
    file = './testdata/data_collector_output_sample_new'

    populate_q(file, 60, offset=3600)
