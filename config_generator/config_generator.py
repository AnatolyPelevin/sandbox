import json
import argparse
from utils.TaskManager import TaskManager
from utils.Utils import Utils
from utils.ETCD import ETCD
import logging

if __name__ == '__main__':
    print("Application started")
    parser = argparse.ArgumentParser()
    parser.add_argument('-u',
                        '--user',
                        nargs=1,
                        help="User for ssh_tunel",
                        type=str,
                        default="")

    parser.add_argument('--password', nargs=1,
                        help="Password for ssh_tunel",
                        type=str,
                        default="")

    parser.add_argument('--attempts', nargs=1,
                        help="Reconnect attempts",
                        type=int,
                        default=5)

    parser.add_argument('--sshhost', nargs=1,
                        help="SSHhost for tunel",
                        type=str,
                        default="sjc01-c01-hdc04.c01.ringcentral.com")

    parser.add_argument('--projectpath', nargs=1,
                        help="JSON tasks file",
                        type=str,
                        default=".")

    parser.add_argument('--tasks', nargs=1,
                        help="JSON tasks",
                        type=str)

    parser.add_argument('--task_file', nargs=1,
                        help="JSON tasks file",
                        type=argparse.FileType('r'))

    parser.add_argument('--etcd_file', nargs=1,
                        help="JSON etcd file",
                        type=argparse.FileType('r'))

    parser.add_argument('--config_path', nargs=1,
                        help="config path",
                        type=str)

    arguments = parser.parse_args()

    tasks ={}
    if arguments.tasks is not None:
        tasks = json.loads(arguments.tasks)
    else:
        tasks = json.load(arguments.task_file[0])

    config = {"DWH_ORACLE_ERD_USER_NAME": None,
              "DWH_ORACLE_ERD_PASSWORD": None,
              "DWH_ORACLE_DATA_SOURCE_URL": None,
              "SFDC_API_USER_NAME": None,
              "SFDC_API_PASSWORD": None,
              "SFDC_API_URL": None,
              "SFDC_HEROKU_POSTGRES_HOST": None,
              "SFDC_HEROKU_POSTGRES_DB_NAME": None,
              "SFDC_HEROKU_POSTGRES_USER": None,
              "SFDC_HEROKU_POSTGRES_PASSWORD": None,
              }

    etcd = ETCD('stage')

    if arguments.etcd_file is not None:
        etcd_json = json.load(arguments.etcd_file[0])
        for item in etcd_json.keys():
            config[item] = etcd_json[item]
    else:
        etcd = ETCD('stage')
        for item in config.keys():
            config[item] = etcd.get_var(item)

    config["user"] = arguments.user[0]
    config["password"] = arguments.password[0]
    config["attempts"] = arguments.attempts[0]
    config["ssh_host"] = arguments.sshhost[0]
    config["pwd"] = arguments.projectpath[0]
    config["config_path"] = arguments.config_path[0]

    utils = Utils()

    taskManager = TaskManager(config)
    taskManager.process(tasks)
