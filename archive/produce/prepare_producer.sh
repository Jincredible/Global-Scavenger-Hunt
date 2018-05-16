#!/bin/bash
# The purpose of this script is to prepare the ingestion cluster to run kafka
# ssh into the master node after starting kafka. then run this script from the home directory of the master node

printf '\npreparing Producer cluster...'
printf '\n'

pip install kafka-python


# The rest of this has been moved to a separate shell script
