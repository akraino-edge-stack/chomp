##############################################################################
# Copyright (c) 2019 AT&T Intellectual Property. All rights reserved.        #
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

FROM frolvlad/alpine-python3
ENV PYTHONPATH=${PYTHONPATH}:/opt/app/org.akraino.chomp/corrlog-count
ENV CONFIG_DIR=/configdir

RUN apk update && apk add bash && apk add bash-completion
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

#Create container directories
RUN mkdir -p /opt/app/org.akraino.chomp/corrlog-count
RUN mkdir -p /configdir

WORKDIR /opt/app/org.akraino.chomp/corrlog-count

#Copy files from context build dir to container build dir
ADD chomp-common .
ADD chomp-corrlog-count .
ADD chomp-corrlog-count/testfiles .

#Install pip packages
RUN pip3 install --upgrade -r requirements.txt

VOLUME /configdir

#Run corrlog-count
CMD ["/opt/app/org.akraino.chomp/corrlog-count/dockerCMD.sh"]
