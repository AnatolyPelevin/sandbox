import json
import argparse
import TaskManager
from utils import Utils, ETCD

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-u',
                        '--user',
                        nargs=1,
                        help="User for ssh_tunel",
                        type=argparse.FileType('r'),
                        default="")

    parser.add_argument('--password', nargs=1,
                        help="Password for ssh_tunel",
                        type=argparse.FileType('r'),
                        default="")

    parser.add_argument('--attempts', nargs=1,
                        help="JSON tasks file",
                        type=argparse.FileType('r'),
                        default=5)

    parser.add_argument('--sshhost', nargs=1,
                        help="JSON tasks file",
                        type=argparse.FileType('r'),
                        default="sjc01-c01-hdc04.c01.ringcentral.com")

    parser.add_argument('--projectpath', nargs=1,
                        help="JSON tasks file",
                        type=argparse.FileType('r'),
                        default=".")

    parser.add_argument('--pathtoconfigs', nargs=1,
                        help="JSON tasks file",
                        type=argparse.FileType('r'),
                        default=".")

    parser.add_argument('--tasks', nargs=1,
                        help="JSON tasks file",
                        type=argparse.FileType('r'))

    arguments = parser.parse_args()
    tasks = json.loads(arguments.tasks[0])

    etcd = ETCD('stage')
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
    for item in config.keys():
        config[item] = etcd.get_var(item)

    config["user"] = arguments.user[0]
    config["password"] = arguments.password[0]
    config["attempts"] = arguments.attempts[0]
    config["ssh_host"] = arguments.sshhost[0]
    config["project_path"] = arguments.projectpath[0]
    config["path_to_projects"] = arguments.pathtoconfigs[0]

    utils = Utils()

    taskManager = TaskManager(config)
    taskManager.process(tasks)
