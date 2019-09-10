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

STAGING_BUILD=${STAGING_BUILD:=''}
AUTOSTAGING=${AUTOSTAGING:=''}

set -e -u -x -o pipefail

# list of chomp containers to build, chomp-redis uses a stock image
containers=("chomp-data-collector" "chomp-corrlog-count" "chomp-singlelog-count" "chomp-kpi-pub")

# relative to workspace
SRCDIR=src
cd ${SRCDIR}

STREAM='master'

# Docker build context will be at SRCDIR to allow common access

for CON_NAME in "${containers[@]}";
do
    echo "-------------------------------------"
    echo "Building image for ${CON_NAME}..."
    echo "-------------------------------------"

    # get common version
    VERSION=$(grep "^version=" chomp-common/version.properties | cut -d'=' -f2)

    if [ -n "$STAGING_BUILD" -a -n "$AUTOSTAGING" ]
    then
        # For a staging build, the $VERSION is fixed
        VERSION=$(echo "$VERSION" | sed 's/-SNAPSHOT//')
        # TODO: put back later
        #DOCKER_REPO='nexus3.akraino.org:10004'
        DOCKER_REPO='localhost:5000/org.akraino.chomp'
    else
        # For a snapshot build - find the latest snapshot
        # TODO: put back later
        #DOCKER_REPO='nexus3.akraino.org:10003'
        DOCKER_REPO='localhost:5000/org.akraino.chomp'
    fi

    # Append stream, if it is not the master stream
    if [ "${STREAM}" != "master" ]
    then
        VERSION="${VERSION}-${STREAM}"
    fi
    
    # Build and push the Docker container
    docker build -f ${CON_NAME}/Dockerfile -t ${CON_NAME}:${VERSION} .
    docker tag ${CON_NAME}:${VERSION} ${DOCKER_REPO}/${CON_NAME}:${VERSION}
    docker push ${DOCKER_REPO}/${CON_NAME}:${VERSION}

    echo "--------------------------------------"
    echo "Done building stuff for ${CON_NAME}..."
    echo "--------------------------------------"
done

exit 0
