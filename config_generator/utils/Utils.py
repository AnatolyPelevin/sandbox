import json
import os
import csv


class Utils:

    @staticmethod
    def readTemplate(path, task):
        key = None
        etl_name = task["CONFIG"].split("-")[0].lower()
        if etl_name == "erd-configs":
            key = ""
        else:
            if etl_name == "sfdc" and task["SOURCE_DB"] == "HEROKU":
                key = "_jdbc"
            else:
                key = "_api"

        with open(path + '/templates/{}_config_template{}.json'.format(etl_name,key)) as json_file:
            template = json.load(json_file)
        return template

    @staticmethod
    def readObjectJson(path):
        with open(path) as json_file:
            object_json = json.load(json_file)
        return object_json

    @staticmethod
    def writeConfig(path, object_json):
        with open(path, 'w') as outfile:
            json.dump(object_json, outfile, indent=4)

    @staticmethod
    def find_field(schema, field_name):
        similar_names = []
        for schema_field in schema.keys():
            distance = Utils.get_distance(schema_field, field_name)
            similar_names.append((schema[schema_field]['field_name'], distance))
        return sorted(similar_names, key=lambda x: x[1])[:3]

    @staticmethod
    def get_distance(a, b):
        n, m = len(a), len(b)
        if n > m:
            a, b = b, a
            n, m = m, n

        current_row = range(n + 1)
        for i in range(1, m + 1):
            previous_row, current_row = current_row, [i] + [0] * n
            for j in range(1, n + 1):
                add, delete, change = previous_row[j] + 1, current_row[j - 1] + 1, previous_row[j - 1]
                if a[j - 1] != b[i - 1]:
                    change += 1
                current_row[j] = min(add, delete, change)

        return current_row[n]

    @staticmethod
    def get_config_path_with_env(config, task, env):
        path = "{config_path}/{config}/{path_in_config}/{env}/".format(
            config_path=config["config_path"],
            config=task['CONFIG'],
            path_in_config='deployment/src/main/resources/include/json/config',
            env=env.lower(),
        )
        return path

    @staticmethod
    def get_path_to_object(config, task, env, object):
        path = "{config_path}/{config}/{path_in_config}/{env}/{object}.json.j2".format(
            config_path=config["config_path"],
            config=task['CONFIG'],
            path_in_config='deployment/src/main/resources/include/json/config',
            env=env.lower(),
            object=object.lower()
        )
        return path

    @staticmethod
    def get_path(config, task):
        path = "{config_path}/{config}/{path_in_config}/{env}/{object}.json.j2".format(
            config_path=config["config_path"],
            config=task['CONFIG'],
            path_in_config='deployment/src/main/resources/include/json/config',
            env=task["ENV"].lower(),
            object=task["OBJECT"].lower()
        )
        return path

    @staticmethod
    def get_all_object_by_path(path):
        object_list = []
        for file in os.listdir(path):
            object_list.append(file.split(".")[0])
        return object_list

    @staticmethod
    def write_csv_report(report, report_columns, report_name):
        csv_file_name = report_name + ".csv"
        abs_path = os.path.abspath(os.path.dirname(__file__))
        path = os.path.join(abs_path, "../reports/" + csv_file_name)
        try:
            with open(path, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=report_columns)
                writer.writeheader()
                for data in report:
                    writer.writerow(data)
        except IOError:
            print("I/O error")
