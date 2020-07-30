import json


class Utils:

    @staticmethod
    def readTemplate(path, etl_name):
        with open(path + '/{}_config_template.json'.format(etl_name) ) as json_file:
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