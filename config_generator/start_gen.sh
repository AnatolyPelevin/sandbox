#!/bin/bash

project_path=$(pwd)
Attempts=5
SSHhost=sjc01-c01-hdc04.c01.ringcentral.com
Tasks_file=${project_path}/input.json
ETCD_file=${project_path}/etcd.json

python3 config_generator.py --task_file="$Tasks_file" \
                            --user="semyon.putnikov" \
                            --password="" \
                            --attempts="$Attempts" \
                            --sshhost="$SSHhost" \
                            --etcd_file="$ETCD_file" \
                            --projectpath="$project_path" \
                            --config_path="/home/semyonputnikov/Documents/projects"