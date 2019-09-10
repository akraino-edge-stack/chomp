#!/bin/bash
#
# Copyright (c) 2019 AT&T Intellectual Property. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Verify script for CI:
#   running lint for python and yaml, unit tests and doc gen.
#   all tests run by 'tox'

set -u 

echo -e "Running verify tests...\n"

# prepare for python tests
virtualenv ./chomp_venv
source ./chomp_venv/bin/activate

run_python_lint() {
    echo "-------------------------------"
    echo " Running Python lint checks:   "
    echo "-------------------------------"

    tox -e lint

    if [ $? -ne 0 ]
    then
        echo "-------------------------------"
        echo " Python lint checks: FAILED    "
        echo "-------------------------------"
        return 1
    else
        echo "-------------------------------"
        echo " Python lint checks: PASSED    "
        echo "-------------------------------"
    fi

    return 0
}

run_yaml_lint() {
    echo "-------------------------------"
    echo " Running YAML lint checks:     "
    echo "-------------------------------"

    tox -e yamllint

    if [ $? -ne 0 ]
    then
        echo "-------------------------------"
        echo " YAML lint checks: FAILED      "
        echo "-------------------------------"
        return 1
    else
        echo "-------------------------------"
        echo " YAML lint checks: PASSED      "
        echo "-------------------------------"
    fi

    return 0
}

run_python_unit_tests() {
    echo "-----------------------"
    echo " Running Unit tests:   "
    echo "-----------------------"

    tox

    if [ $? -ne 0 ]
    then
        echo "------------------------"
        echo " Unit tests: FAILED     "
        echo "------------------------"
        return 1
    else
        echo "------------------------"
        echo " Unit tests: PASSED     "
        echo "------------------------"
    fi

    return 0
}

run_docs_gen() {
    echo "-------------------------------"
    echo " Running Docs gen:             "
    echo "-------------------------------"

    tox -e docs

    if [ $? -ne 0 ]
    then
        echo "-------------------------------"
        echo " Docs gen: FAILED              "
        echo "-------------------------------"
        return 1
    else
        echo "-------------------------------"
        echo " Docs gen: PASSED              "
        echo "-------------------------------"
    fi

    return 0
}

# main

rc=0

# Python lint
echo -e "\n"
run_python_lint
rc=$(( $rc | $? ))

# YAML lint
echo -e "\n"
run_yaml_lint
rc=$(( $rc | $? ))

# Unit tests
echo -e "\n"
run_python_unit_tests
rc=$(( $rc | $? ))

# Doc gen
echo -e "\n"
run_docs_gen
rc=$(( $rc | $? ))

deactivate

exit $rc
