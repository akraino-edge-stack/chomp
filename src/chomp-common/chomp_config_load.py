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
from json import load, loads
from akrainoLogger import akraino_logger


def frameInfo(frame):
    return os.path.basename(frame.f_code.co_filename) + \
        "|" + str(frame.f_lineno) + "|" + frame.f_code.co_name


logger = akraino_logger()


def getconfigfile(func, basedir=os.getenv("CONFIG_DIR")):

    if basedir is None:
        basedir = '.'
    configfile = basedir + '/' + func + '.yaml'
    return configfile


def load_config(configfile):

    logger.info("config file {}"
                .format(configfile),
                frameInfo(sys._getframe()))

    # define app config
    appconfig = {}

    try:
        with open(configfile, 'r') as stream:
            appconfig = yaml.load(stream)

    except FileNotFoundError:
        logger.error("config file \"{}\" not found"
                     .format(configfile),
                     frameInfo(sys._getframe()))
        raise
    except Exception:
        logger.error("config file \"{}\" failed to load"
                     .format(configfile),
                     frameInfo(sys._getframe()))
        raise

    # convert string intervals to seconds, e.g. "1m" -> 60, "1h" -> 3600, "1s" -> 1
    appconfig['period'] = convert_interval_to_s(appconfig['period'])
    appconfig['period'] = 60 if not isinstance(appconfig['period'], int) else appconfig['period']
    for key in appconfig.keys():
        if 'offset' in key.lower():
            appconfig[key] = convert_interval_to_s(appconfig[key])
            appconfig[key] = 60 if not isinstance(appconfig[key], int) else appconfig[key]
    if appconfig['period'] < 60:
        logger.error("period set below 60s, exiting",
                     frameInfo(sys._getframe()))
        exit(0)
    return appconfig


def load_signatures(sigpath):
    signatures = []
    try:
        files = os.listdir(sigpath)
    except Exception:
        logger.error("path \"{}\" does not exist"
                     .format(sigpath),
                     frameInfo(sys._getframe()))
        exit(1)

    for file in files:
        infile = sigpath + '/' + file
        with open(infile) as f:
            signature = {}
            try:
                signature = load(f)
                logger.info("signature file {} successfully loaded"
                            .format(infile),
                            frameInfo(sys._getframe()))
            except UnicodeDecodeError:
                logger.error("signature file {} is malformed json"
                             .format(infile),
                             frameInfo(sys._getframe()))
                continue
            except Exception:
                logger.error("signature file {} fails to load"
                             .format(infile),
                             frameInfo(sys._getframe()))
                continue
            if signature != {}:
                try:
                    if signature['ignore'].lower() == "yes":
                        continue
                except Exception:
                    pass
                try:
                    signature['query'] = loads(signature['query'])
                    signatures.append(signature)
                except Exception:
                    logger.error("query for signature \'{}\' in file {} malformed, signature being ignored"
                                 .format(signature['name'], infile),
                                 frameInfo(sys._getframe()))

    return signatures


def load_signatures_siglist(sigpath, basedir=os.getenv("CONFIG_DIR")):

    if basedir is None:
        basedir = '.'
    fqsigpath = basedir + '/' + sigpath

    signatures = []
    files = []
    try:
        with open(fqsigpath) as f:
            for line in f:
                if line.lstrip() == '':
                    continue
                if line.lstrip()[0] == '#':
                    continue
                files.append(basedir + '/' + line.lstrip().rstrip())
                logger.info(line, frameInfo(sys._getframe()))
    except Exception:
        logger.error("path \"{}\" does not exist"
                     .format(fqsigpath),
                     frameInfo(sys._getframe()))
        return signatures

    for file in files:
        infile = file
        with open(infile) as f:
            signature = {}
            try:
                signature = load(f)
                logger.info("signature file {} successfully loaded"
                            .format(infile),
                            frameInfo(sys._getframe()))
            except UnicodeDecodeError:
                logger.error("signature file {} is malformed json"
                             .format(infile),
                             frameInfo(sys._getframe()))
            except Exception:
                logger.error("signature file {} fails to load"
                             .format(infile),
                             frameInfo(sys._getframe()))
            if signature != {}:
                try:
                    if signature['ignore'].lower() == "yes":
                        continue
                except Exception:
                    pass
                try:
                    signature['query'] = loads(signature['query'])
                    signatures.append(signature)
                except Exception:
                    logger.error("query for signature \'{}\' in file {} malformed, signature being ignored"
                                 .format(signature['name'], infile),
                                 frameInfo(sys._getframe()))

    return signatures


def load_regex(signatures, regexfile, basedir=os.getenv("CONFIG_DIR")):

    if basedir is None:
        basedir = '.'
    fqregexfile = basedir + '/' + regexfile

    regexlist = {}
    returnsigs = []
    try:
        with open(fqregexfile) as f:
            for line in f:
                line = line.lstrip().rstrip()
                if line == '' or ',' not in line or line[0] == "#":
                    continue
                regexlist[line[:line.find(',')]] = line[line.find(',')+1:]
    except Exception:
        pass
    # logger.info("regexlist: {}".format(regexlist), frameInfo(sys._getframe()))

    for signature in signatures:
        if 'regex' in signature.keys():
            regexreplace = {}
            if isinstance(signature['regex'], dict):
                for key in signature['regex'].keys():
                    if signature['regex'][key] in regexlist.keys():
                        regexreplace[key] = regexlist[signature['regex'][key]]
                signature['regex'] = regexreplace
            else:
                signature.pop('regex')

        returnsigs.append(signature)
    return returnsigs


def is_int(string):
    try:
        int(string)
        return True
    except Exception:
        return False


def is_float(string):
    try:
        float(string)
        return True
    except Exception:
        return False


def convert_interval_to_s(string):
    '''
    converts a timestring to seconds
    :param string:
    :return:
    '''
    if is_int(string) or isinstance(string, int):
        logger.info("yes", frameInfo(sys._getframe()))
        return int(string)
    time_in_s = 0
    prev_val = ''
    for val in string:
        if is_int(val):
            prev_val += val
        else:
            if val == 's' or val == 'S':
                time_in_s += int(prev_val) * 1
            elif val == 'm' or val == 'M':
                time_in_s += int(prev_val) * 60
            elif val == 'h' or val == 'H':
                time_in_s += int(prev_val) * 3600
            elif val == 'd' or val == 'D':
                time_in_s += int(prev_val) * 3600 * 24
            elif val == 'w' or val == 'W':
                time_in_s += int(prev_val) * 3600 * 24 * 7
            else:
                logger.warning("invalid value {}"
                               .format(val),
                               frameInfo(sys._getframe()))
                return False
            prev_val = ''

    return time_in_s
