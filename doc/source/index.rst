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

.. note::

  These documents will be reworked to reflect the changes associated with
  becoming an Akraino hosted project: CHOMP. Expect major changes to occur
  with time. 

.. toctree::

    How To <howto>

CHOMP
=======

CHOMP (Cluster Health Overload Monitoring Platform)
is a collection of components that monitor/analyze log records stored in 
Elasticsearch DB. CHOMP collects log records,
analyzes the log records using user-configured signature files
to perform event correlation,
and finally persists the results into Elasticsearch DB for later use. 

Approach
--------

CHOMP employs user-configurable signatures, expressed as regular expressions,
to perform event correlation to track event success/failure rates and event latency.

Building this Documentation
---------------------------

The docs are built during the verify step.
Verify will build a html version of this documentation that can
be viewed using a browser at doc/build/index.html on the local filesystem.

Specification Details
---------------------

Proposed, approved, and implemented specifications for
the CHOMP project will be provided.

How To Information
------------------

To learn how to use CHOMP, please refer to howto_ .

Conventions and Standards
-------------------------

.. _Helm: https://helm.sh/
.. _Kubernetes: https://kubernetes.io/
.. _Openstack: https://www.openstack.org/
.. _Openstack-Helm: https://docs.openstack.org/openstack-helm/latest/
.. _yaml: http://yaml.org/
.. _howto: doc/build/howto.html
