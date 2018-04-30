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

printf '\nrunning pegasus installation scrpts on producer clusters\n'

BASEDIR=$(dirname "$0")

CLUSTER_NAME=produce-cluster


peg fetch ${CLUSTER_NAME}

read -p "Are you sure to install packages on your instances? Y/n" -n 1 -r
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

read -p "Press enter to install spark:"
# install and start spark
peg install ${CLUSTER_NAME} spark
#peg service ${CLUSTER_NAME} spark start

read -p "Press enter to install zookeeper:"
# install and start zookeeper
peg install ${CLUSTER_NAME} zookeeper
#peg service ${CLUSTER_NAME} zookeeper start

read -p "Press enter to install kafka:"
# install and start kafka
peg install ${CLUSTER_NAME} kafka
#peg service ${CLUSTER_NAME} kafka start

printf '\ninstalled all packages\n'
fi