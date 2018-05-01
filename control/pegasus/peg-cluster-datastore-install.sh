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
# call pegasus commands on the user-defined clusters to install packages

printf '\nrunning pegasus installation scripts on datastore clusters\n'

BASEDIR=$(dirname "$0")

CLUSTER_NAME=process-cluster


peg fetch ${CLUSTER_NAME}

read -p "Are you sure to install packages on your instances? Y" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then

printf '\ninstalling ssh and aws to the cluster:'
peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws

read -p "Press enter to install environment packages:"
peg install ${CLUSTER_NAME} environment 
#[WARNINGS WHEN INSTALLING ENVIRONMENT]:
#1. maven can't be installed
#2. recommend to upgrade pip

read -p "Press enter to install hadoop:"
# install and start hadoop
peg install ${CLUSTER_NAME} hadoop
#peg service ${CLUSTER_NAME} hadoop start

read -p "Press enter to install cassandra:"
# install and start cassandra
peg install ${CLUSTER_NAME} cassandra
#peg service ${CLUSTER_NAME} cassandra start

read -p "Press enter to install redis:"
# install and start redis
peg install ${CLUSTER_NAME} redis
#peg service ${CLUSTER_NAME} redis start



printf '\ninstalled all packages\n'
fi