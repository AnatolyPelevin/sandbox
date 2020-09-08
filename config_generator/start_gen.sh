#!/bin/bash

project_path=$(pwd)
my_password=$(cat pass)
SSHhost=sjc01-c01-hdc09.c01.ringcentral.com
Tasks_file=${project_path}/tasks/input.json
ETCD_file=${project_path}/etcd.json

python3 config_generator.py --task_file="$Tasks_file" \
                            --user="semyon.putnikov" \
                            --password="$my_password" \
                            --sshhost="$SSHhost" \
                            --etcd_file="$ETCD_file" \
                            --projectpath="$project_path" \
                            --config_path="/home/semyonputnikov/Documents/projects"