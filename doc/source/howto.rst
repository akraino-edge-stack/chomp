..
      Copyright 2019 AT&T Intellectual Property.
      All Rights Reserved.

      Licensed under the Apache License, Version 2.0 (the "License"); you may
      not use this file except in compliance with the License. You may obtain
      a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

      Unless required by applicable law or agreed to in writing, software
      distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
      WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
      License for the specific language governing permissions and limitations
      under the License.

.. 

CHOMP How-To
============

Introduction
------------

The CHOMP platform allows users to specify search criteria patterns to query
log records from the `Elasticsearch`_ DB. The search results are processed
to label a log record with a signature name and use user-specified regular
expressions in later processing. Once labelled, the signatures can be processed
in a single event or correlated event manner. Some `Kubernetes`_ activities 
onlygenerate a single log record indicating success or failure, while other
activities generate a sequence of log records that need to be correlated
to determine success or failure. Activity latency information is usually
available or can be calculated.

The design of CHOMP has the search criteria and regular expression-related
files contained in the `Helm`_ Chart's "files" directory. This is where the
configmap template `YAML`_ files process these files and makes them available to the
corresponding container.

For the Helm Chart, the "values.yaml" file has the platform's configuration
values. Some of these need to be configured prior to "helm install". The
name/value pairs with "CHANGE_ME" are required fields to be populated with 
correct values for your environment.

A `Redis`_ container is used to communicate data, using pub/sub, between
components.

The initial CHOMP component, data-collector, does the DB query and collects
log records. There are three configuration elements that are used to control
processing:

1. a "signatures" directory that contains JSON files containing the signature
   name, search criteria, and other information,
#. a "regex_list.txt" file that contains regular expressions to pull values
   from a log record,
#. a "signaturelist" file that contains a list of signature files in the 
   "signatures" directory.
 
Another CHOMP component, singlelog-count, handles events characterized by
a single log record. There are two configuration elements that are used to
control processing:

1. a "counters" directory that contains JSON files containing the event name,
   and other information,
#. a "counters_list" file that contains a list of event success and fail
   files in the "counters" directory.
 
Another CHOMP component, corrlog-count, handles the correlation of multiple
events. There are two configuration elements that are used to control
processing:

1. a "corrlogsignatures" directory that contains JSON files containing the
   event name, correlation criteria, and other information,
#. a "corrliglist" file that contains a list of signature files in the 
   "corrlogsignatures" directory.
 
The last CHOMP component, kpi-pub, collects, packages and sends the success,
fail, and latency information to an upload API on the Elasticsearch DB.

User Story 1
------------

As a Kubernetes operator, I want to get a pulse on pod creates, so that I
get an early indication that there might be a problem.

**data-collector**

For data-collector, you might want to set up the following files.
**NOTE**: since Helm configmap is unable to support key names with '/',
directories aren't supported and you need to substitute '_' for '/', below.

files/data-collector/regex_list.txt:

.. code-block:: text

    apiserver_lat,.+\((.+)s\).+
    apiserver_pod,.+\/pods\/(.+)\:\s\(
    apiserver_namespace,.+\/namespaces\/(.+)\/pods
    kubelet_pleg_pod,.+SyncLoop\s+\(PLEG\):\s\"([a-zA-Z0-9-]+).+
    kubelet_pleg_namespace,.+SyncLoop\s+\(PLEG\):\s\"[a-zA-Z0-9-]+\_([a-zA-Z0-9-]+)\(

files/data-collector/signaturelist:

.. code-block:: text

    signatures_sig001.json

files/data-collector/signatures/sig001.json:

.. code-block:: json

    {
      "name":"sig_001",
      "ignore":"no",
      "event_name":["pod_create_e2e_succ", "pod_create_e2e_fail", "pod_create_e2e_complete"],
      "application":"K-CHOMP",
      "type":["single", "correlated"],
      "query":"{\"query\": {\"bool\": {\"must\": [{\"match_phrase\": {\"log\": \"GET\"}}, {\"match_phrase\": {\"log\":\"pods\"}}, {\"match_phrase\": {\"log\": \"namespaces\"}}]}}}",
      "regex":{
        "pod":"apiserver_pod",
        "namespace":"apiserver_namespace",
        "latency":"apiserver_lat"
      }
    }

**singlelog-count**

For singlelog-count, you might want to set up the following files.
**NOTE**: since Helm configmap is unable to support key names with '/',
directories aren't supported and you need to substitute '_' for '/', below.

files/singlelog-count/counters_list:

.. code-block:: text

    counters_pod_create_api_req_succ.json
    counters_pod_create_api_req_fail.json

files/singlelog-count/counters/pod_create_api_req_fail.json:

.. code-block:: json

    {
      "name": "pod_create_api_req_fail",
      "type": "single",
      "signatures": [
        "sig_002"
      ]
    }

files/singlelog-count/counters/pod_create_api_req_succ.json:

.. code-block:: json

    {
      "name": "pod_create_api_req_succ",
      "type": "single",
      "signatures": [
        "sig_001"
      ]
    }


**corrlog-count**

For corrlog-count, you might want to set up the following files.
**NOTE**: since Helm configmap is unable to support key names with '/',
directories aren't supported and you need to substitute '_' for '/', below.

files/corrlog-count/corrsiglist:

.. code-block:: text

    corrlogsignatures_pod_create_e2e_succ.json

files/corrlog-count/corrlogsignatures/pod_create_e2e_succ.json:

.. code-block:: json

    {
      "name": "pod_create_e2e_succ",
      "match": "pod",
      "application": "k8s telemetry",
      "latency": true,
      "corr_type": 1,
      "keep_succ_logs": true,
      "keep_fail_logs": true,
      "sig_list": [
        "not sig_003",
        "sig_001",
        "sig_002",
        "sig_001"
      ],
      "time_limit": 10
    }

.. _Elasticsearch: https://elastic.co/
.. _Helm: https://helm.sh/
.. _Kubernetes: https://kubernetes.io/
.. _Redis: https://redis.io/
.. _YAML: http://yaml.org/
