from reader import Reader, OracleReader, SalesForceReader, HerokuReader
from utils import Utils
import json
import os.path
import os
import copy
import logging


class TaskManager:

    def __init__(self,
                 config):
        self.config = config

    def defineTask(self, task, full_path):
        task_name = task["TASK"]
        tasks_dict = {
            "ADD FIELDS": self.addFields,
            "REMOVE FIELDS": self.removeFields,
            "CHANGE OBJECT CONFIG": self.changeConfig,
            "REMOVE OBJECT": self.removeObject,
            "CHECK DATATYPE CAST": self.checkDataCast
        }
        if task_name in tasks_dict.keys():
            return tasks_dict[task_name](task, full_path)
        else:
            raise NotImplementedError

    def defineReader(self, task):
        source_name = task["SOURCE_DB"]
        source_dict = {
            "ORACLE": OracleReader,
            "SALESFORCE": SalesForceReader,
            "HEROKU": HerokuReader
        }
        if source_name in source_dict.keys():
            return source_dict[source_name](self.config)
        else:
            raise NotImplementedError

    def getFieldContent(self, reader, task, column_name, field_name, source_schema, column_order):
        fields_dict = {
            "HIVE_NAME": self.getHiveName,
            "SOURCE_NAME": self.getSourceName,
            "DESTINATION_NAME": self.getDestinationName,
            "DATA_TYPE": self.getDataType,
            "COLUMN_ORDER": self.getColumnOrder,
            "IS_NULL": self.getIsNullFlag,
            "INGESTION_TYPE": self.getIngectionType,
            "MISC": self.getMisc,
        }
        if field_name in fields_dict.keys():
            return fields_dict[field_name](reader, task, column_name, source_schema, column_order)
        else:
            raise NotImplementedError

    def getHiveName(self, reader, task, column_name, source_schema, column_order):
        return column_name

    def getSourceName(self, reader, task, column_name, source_schema, column_order):
        return column_name

    def getDestinationName(self, reader, task, column_name, source_schema, column_order):
        if "ALL" not in task["ATTRIBUTES"]["FIELDS"] and task["ATTRIBUTES"]["FIELDS"][column_name] is not None:
            return task["ATTRIBUTES"]["FIELDS"][column_name]
        else:
            return column_name

    def getDataType(self, reader, task, column_name, source_schema, column_order):
        source_type = source_schema[column_name]["type"]
        length = source_schema[column_name]["length"]
        return reader.getDataType(source_type, length, column_name)

    def getColumnOrder(self, reader, task, column_name, source_schema, column_order):
        return column_order

    def getIsNullFlag(self, reader, task, column_name, source_schema, column_order):
        return False

    def getIngectionType(self, reader, task, column_name, source_schema, column_order):
        ingestion_types_dict = {
            "SalesForceReader": "API",
            "HerokuReader": "JDBC"
        }
        reader_name = type(reader).__name__
        if reader_name in ingestion_types_dict.keys():
            return ingestion_types_dict[reader_name]
        else:
            raise NotImplementedError

    def getMisc(self, reader, task, column_name, source_schema, column_order):
        return {}

    def getMaxColumnOrder(self, object_json):
        column_orders = [item['COLUMN_ORDER'] for item in object_json['FIELDS']]
        count = 0 if not column_orders else max(column_orders)
        return count

    def check_duplicates(self, object_config, fields_dict):
        object_config_fields_name = [item['HIVE_NAME'] for item in object_config['FIELDS']]
        checked_fields = {}
        fields_list = fields_dict.keys()
        for item in fields_list:
            if item in object_config_fields_name:
                print("Field %s for object %s is in config" % (item, object_config["HIVE_TABLE_NAME"]))
            else:
                checked_fields.update({item: fields_dict[item]})
        return checked_fields

    def verifyFields(self, fields_dict, source_schema, object_json):
        checked_fields_dict = {}
        print(source_schema)
        for item in fields_dict.keys():
            if item not in source_schema.keys():
                raise ValueError("No field %s for object %s at source" % (item, object_json["HIVE_TABLE_NAME"]))
            else:
                checked_fields_dict.update({item: fields_dict[item]})
        result = self.check_duplicates(object_json, checked_fields_dict)
        return result

    def createObjectFromTemplate(self, task):
        utils = Utils()
        assert "HIVE_TABLE_NAME" in task["ATTRIBUTES"].keys()
        assert "TABLE_QUERY" in task["ATTRIBUTES"].keys()
        assert "DESTINATION_TABLE_NAME" in task["ATTRIBUTES"].keys()
        object_json = utils.readTemplate(self.config["project_path"], task["CONFIG"].split("-")[0].lower())
        object_json["HIVE_TABLE_NAME"] = task["ATTRIBUTES"]["HIVE_TABLE_NAME"]
        object_json["TABLE_QUERY"] = task["ATTRIBUTES"]["TABLE_QUERY"]
        object_json["DESTINATION_TABLE_NAME"] = task["ATTRIBUTES"]["DESTINATION_TABLE_NAME"]
        return object_json

    def updateTask(self, task, object_json):
        if "SCHEMA" not in task.keys():
            if len(object_json["TABLE_QUERY"].split(".")) > 1:
                task["SCHEMA"] = object_json["TABLE_QUERY"].split(".")[0]
            else:
                task["SCHEMA"] = ""

        if "SOURCE" not in task.keys():
            if len(object_json["TABLE_QUERY"].split(".")) > 1:
                task["SOURCE"] = object_json["TABLE_QUERY"].split(".")[1]
            else:
                task["SOURCE"] = object_json["TABLE_QUERY"]
        return task

    def getAllFields(self, source_schema):
        result = {item: None for item in source_schema.keys()}
        return result

    def process(self, tasks):
        utils = Utils()
        path_in = 'deployment/src/main/resources/include/json/config'
        for task in tasks:
            print(json.dumps(task, indent=4))

            full_path = "%s/%s/%s/%s/%s.json.j2" % (self.config["path_to_projects"],
                                                    task['CONFIG'],
                                                    path_in,
                                                    task["ENV"].lower(),
                                                    task["OBJECT"].lower()
                                                    )

            result = self.defineTask(task, full_path)

            print("\n")
            print(json.dumps(result, indent=4))
            print("\n")

            if result is not None:
                utils.writeConfig(full_path, result)

    def removeFields(self, task, full_path):
        utils = Utils()
        object_json = {}
        if os.path.exists(full_path):
            object_json = utils.readObjectJson(full_path)
        else:
            raise ValueError("Object %s not found" % (task["OBJECT"],))

        assert task["ENV"] is not None and task["ENV"] in ["RC", "ATT", "UAT"]
        assert "FIELDS" in task["ATTRIBUTES"] and len(task["ATTRIBUTES"]["FIELDS"]) > 0

        fields_dict = task["ATTRIBUTES"]["FIELDS"]
        for item in fields_dict.keys():
            order = [i for i in range(len(object_json["FIELDS"])) if object_json["FIELDS"][i]["HIVE_NAME"] == item][0]
            object_json["FIELDS"][order]["IS_NULL"] = True
        return object_json

    def changeConfig(self, task, full_path):
        utils = Utils()
        object_json = {}

        if os.path.exists(full_path):
            object_json = utils.readObjectJson(full_path)
            task = self.updateTask(task, object_json)
        else:
            raise ValueError("Object %s not found" % (task["OBJECT"],))

        for item in task["ATTRIBUTES"].keys():
            object_json[item] = task["ATTRIBUTES"][item]
        return object_json

    def removeObject(self, task, full_path):
        utils = Utils()
        object_json = {}

        if os.path.exists(full_path):
            object_json = utils.readObjectJson(full_path)
        else:
            raise ValueError("Object %s not found" % (task["OBJECT"],))

        object_json["ENABLED"] = False
        return object_json

    def checkDataCast(self, task, full_path):
        utils = Utils()
        object_json = {}

        if os.path.exists(full_path):
            object_json = utils.readObjectJson(full_path)
        else:
            raise ValueError("Object %s not found" % (task["OBJECT"],))

        reader = self.defineReader(task)
        source_schema = reader.getSchema(task["SOURCE"].upper(), task["SCHEMA"])
        object_fields_names = {item["HIVE_NAME"]: item["DATA_TYPE"] for item in object_json["FIELDS"]}

        for item in object_fields_names.keys():
            schema_datatype = source_schema[item]["type"]
            config_type = object_fields_names[item]
            generated_data_type = self.getDataType(reader, None, item, source_schema)
            if config_type != generated_data_type:
                logging.info("TypeMissCast: %s - config_type: %s, generated_type: %s, schema_type: %s" % (item, config_type, generated_data_type, schema_datatype))

        return None

    def addFields(self, task, full_path):
        utils = Utils()
        object_json = {}

        if os.path.exists(full_path):
            object_json = utils.readObjectJson(full_path)
            task = self.updateTask(task, object_json)
        else:
            object_json = self.createObjectFromTemplate(task)

        assert task["SOURCE_DB"] in ["ORACLE", "HEROKU", "SALESFORCE"]
        assert task["ENV"] in ["RC", "ATT", "UAT"]
        assert ("FIELDS" in task["ATTRIBUTES"] and len(task["ATTRIBUTES"]["FIELDS"]) > 0) or "ALL" in task[
            "ATTRIBUTES"]

        reader = self.defineReader(task)

        source_schema = reader.getSchema(task["SOURCE"].upper(), task["SCHEMA"])
        fields_dict = {}

        if "ALL" in task["ATTRIBUTES"]["FIELDS"] and task["ATTRIBUTES"]["FIELDS"]["ALL"]:
            fields_dict.update(self.verifyFields(self.getAllFields(source_schema), source_schema, object_json))
        else:
            fields_dict.update(self.verifyFields(task["ATTRIBUTES"]["FIELDS"], source_schema, object_json))

        fields_template = \
        utils.readTemplate(self.config["project_path"], task["CONFIG"].split("-")[0].lower())["FIELDS"][0]
        column_order = self.getMaxColumnOrder(object_json)
        print(fields_dict)
        for item in fields_dict.keys():
            column_order += 1
            fields_template_copy = copy.deepcopy(fields_template)
            for field_name in fields_template_copy:
                fields_template_copy[field_name] = self.getFieldContent(reader,
                                                                        task,
                                                                        item,
                                                                        field_name,
                                                                        source_schema,
                                                                        column_order)
            object_json["FIELDS"].append(fields_template_copy)
        return object_json
