#!/bin/bash

# Copyright 2015 Insight Data Science
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Slightly edited by Steven Jin for Insight Project - Global Scavenger Hunt

# PURPOSE OF THIS SCRIPT
# call pegasus commands on the user-defined *.yml files to deploy instances

BASEDIR=$(dirname "$0")

CLUSTER_NAME=ingest-cluster

peg validate ${BASEDIR}/cluster-ingest-master.yml
peg validate ${BASEDIR}/cluster-ingest-workers.yml

read -p "Are you sure to deploy the clusters? Y/n" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
peg up ${BASEDIR}/cluster-ingest-master.yml &
peg up ${BASEDIR}/cluster-ingest-workers.yml &

wait

print 'created all clusters'

peg fetch ${CLUSTER_NAME}

fi