import json


class Utils:

    def readTemplate(self, project_path, etl_name):
        with open(project_path + '/%s_config_template.json' % (etl_name,)) as json_file:
            template = json.load(json_file)
        return template

    def readObjectJson(self, path):
        with open(path) as json_file:
            object_json = json.load(json_file)
        return object_json

    def writeConfig(self, path, object_json):
        with open(path, 'w') as outfile:
            json.dump(object_json, outfile, indent=4)