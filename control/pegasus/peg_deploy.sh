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

# Slightly edited by Steven Jin for Insight Project - Scavenger Hunt

# PURPOSE OF THIS SCRIPT
# call pegasus commands on the user-defined *.yml files to deploy instances

# SPARK ===================================================================


printf '\ndeploying spark cluster\n'

BASEDIR=$(dirname "$0")

CLUSTER_NAME=spark

peg validate ${BASEDIR}/spark-master.yml
peg validate ${BASEDIR}/spark-workers.yml

read -p "Are you sure to deploy the clusters? Y/n" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
peg up ${BASEDIR}/spark-master.yml &
peg up ${BASEDIR}/spark-workers.yml &

wait

printf '\ncreated all clusters. fetching data for cluster\n'

peg fetch ${CLUSTER_NAME}

fi

# KAFKA =============================================================

printf '\ndeploying kafka cluster\n'

CLUSTER_NAME=kafka

peg validate ${BASEDIR}/kafka-master.yml
peg validate ${BASEDIR}/kafka-workers.yml

read -p "Are you sure to deploy the clusters? Y/n" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
peg up ${BASEDIR}/kafka-master.yml &
peg up ${BASEDIR}/kafka-workers.yml &

wait

printf '\ncreated all clusters. fetching data for cluster\n'

peg fetch ${CLUSTER_NAME}

# KAFKA =============================================================

printf '\ndeploying redis instance\n'

CLUSTER_NAME=redis

peg validate ${BASEDIR}/redis-master.yml

read -p "Are you sure to deploy the clusters? Y/n" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
peg up ${BASEDIR}/redis-master.yml &

wait

printf '\ncreated all clusters. fetching data for cluster\n'

peg fetch ${CLUSTER_NAME}
