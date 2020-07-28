import json
import argparse
from utils.TaskManager import TaskManager
from utils.ETCD import ETCD
import logging

if __name__ == '__main__':
    logging.info("Application started")
    parser = argparse.ArgumentParser()
    parser.add_argument('-u',
                        '--user',
                        help="User for ssh_tunel",
                        type=str,
                        default="")

    parser.add_argument('--password',
                        help="Password for ssh_tunel",
                        type=str,
                        default="")

    parser.add_argument('--attempts',
                        help="Reconnect attempts",
                        type=int,
                        default=5)

    parser.add_argument('--sshhost',
                        help="SSHhost for tunel",
                        type=str,
                        default="sjc01-c01-hdc04.c01.ringcentral.com")

    parser.add_argument('--projectpath',
                        help="Absolute path to project",
                        type=str,
                        default=".")

    parser.add_argument('--tasks',
                        help="JSON tasks",
                        type=str)

    parser.add_argument('--task_file',
                        help="JSON tasks file",
                        type=argparse.FileType('r'))

    parser.add_argument('--etcd_file',
                        help="JSON etcd file",
                        type=argparse.FileType('r'))

    parser.add_argument('--config_path',
                        help="Absolute path to config",
                        type=str)

    arguments = parser.parse_args()

    if arguments.tasks is not None:
        tasks = json.loads(arguments.tasks)
    else:
        tasks = json.load(arguments.task_file)

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

    if arguments.etcd_file:
        etcd_json = json.load(arguments.etcd_file)
        for item in etcd_json.keys():
            config[item] = etcd_json[item]
    else:
        etcd = ETCD('stage')
        for item in config.keys():
            config[item] = etcd.get_var(item)

    config["user"] = arguments.user
    config["password"] = arguments.password
    config["attempts"] = arguments.attempts
    config["ssh_host"] = arguments.sshhost
    config["pwd"] = arguments.projectpath
    config["config_path"] = arguments.config_path

    taskManager = TaskManager(config)
    taskManager.process(tasks)
    logging.info("Application finished")
