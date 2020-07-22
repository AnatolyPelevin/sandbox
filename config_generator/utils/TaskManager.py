from readers.OracleReader import OracleReader
from readers.SalesForceReader import SalesForceReader
from readers.HerokuReader import HerokuReader
from utils.Utils import Utils
from utils.ConfigBuilder import ConfigBuilder
import os.path
import os
import copy
import logging


class TaskManager:

    def __init__(self,
                 config,
                 utils=None):
        self.config = config
        self.utils = Utils()

    def defineTask(self, task, full_path):
        task_name = task["TASK"]
        tasks_dict = {
            "ADD FIELDS": self.addFields,
            "REMOVE FIELDS": self.removeFields,
            "CHANGE OBJECT CONFIG": self.changeConfig,
            "REMOVE OBJECT": self.removeObject,
            "CHECK DATATYPE CAST": self.checkDataCast,
            "CHECK SALESFORCE API NAME": self.checkSalesForceApiName
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
                logging.warning(
                    "DuplicatedField: Field '%s' for object '%s' is in config" % (
                        item, object_config["HIVE_TABLE_NAME"]))
            else:
                checked_fields.update({item: fields_dict[item]})
        return checked_fields

    def verifyFields(self, fields_dict, source_schema, object_json):
        checked_fields_dict = {}
        for item in fields_dict.keys():
            if item not in source_schema.keys():
                logging.error("No field %s for object %s at source" % (item, object_json["HIVE_TABLE_NAME"]))
            else:
                checked_fields_dict.update({item: fields_dict[item]})
        result = self.check_duplicates(object_json, checked_fields_dict)
        return result

    def updateTask(self, task, object_json):
        if "SOURCE" not in task.keys():
            if len(object_json["TABLE_QUERY"].split(".")) > 1:
                task["SOURCE"] = object_json["TABLE_QUERY"].split(".")[1]
            else:
                task["SOURCE"] = object_json["TABLE_QUERY"]
            logging.info("Update 'SOURCE' field from object config for task with number: %s" % (task["NUMBER"],))

        return task

    def getAllFields(self, source_schema):
        result = {item: None for item in source_schema.keys()}
        return result

    def process(self, tasks):
        path_in = 'deployment/src/main/resources/include/json/config'
        for task in tasks:
            try:
                assert task["TASK"] is not None, logging.error(
                    "'TASK' field of task with number %s is wrong" % (task["NUMBER"],))
                assert task["CONFIG"] is not None and task["CONFIG"] in ["erd-configs", "sfdc-config"], logging.error(
                    "'CONFIG' field of task with number %s is wrong" % (task["NUMBER"],))
                assert task["OBJECT"] is not None, logging.error(
                    "'OBJECT' field of task with number %s is wrong" % (task["NUMBER"],))
                assert task["ENV"] is not None and task["ENV"] in ["RC", "ATT", "UAT"], logging.error(
                    "'ENV' field of task with number %s is wrong" % (task["NUMBER"],))
            except AssertionError:
                logging.error("SkippedTask: %s - wrong task config" % (task["NUMBER"],))
                continue
            logging.info("Start task %s" % (task["NUMBER"]))
            full_path = "%s/%s/%s/%s/%s.json.j2" % (self.config["config_path"],
                                                    task['CONFIG'],
                                                    path_in,
                                                    task["ENV"].lower(),
                                                    task["OBJECT"].lower()
                                                    )

            result = self.defineTask(task, full_path)

            if result:
                logging.info("SUCCESS: Task %s was completed successfully" % (task["NUMBER"],))
            else:
                logging.error("FAILED: Task %s was completed unsuccessfully" % (task["NUMBER"],))

    def removeFields(self, task, full_path):
        config_is_changed = False
        object_json = {}
        if os.path.exists(full_path):
            object_json = self.utils.readObjectJson(full_path)
        else:
            logging.error("SkippedTask: %s - object config not found" % (task["NUMBER"],))
            return False

        try:
            assert "FIELDS" in task["ATTRIBUTES"] and len(task["ATTRIBUTES"]["FIELDS"]) > 0, logging.error(
                "'FIELDS' field of task with number %s is wrong" % (task["NUMBER"],))
        except AssertionError:
            logging.error("SkippedTask: %s - wrong task config" % (task["NUMBER"],))
            return False

        logging.info("Start task with number: %s" % (task["NUMBER"],))

        fields_dict = task["ATTRIBUTES"]["FIELDS"]
        for item in fields_dict.keys():
            order = [i for i in range(len(object_json["FIELDS"])) if object_json["FIELDS"][i]["HIVE_NAME"] == item][0]
            if not object_json["FIELDS"][order]["IS_NULL"]:
                object_json["FIELDS"][order]["IS_NULL"] = True
                config_is_changed = True

        if config_is_changed:
            self.utils.writeConfig(full_path, object_json)
        else:
            logging.error(
                "NoChanges: %s - config has not changed" % (task["NUMBER"],))

        return True

    def changeConfig(self, task, full_path):
        config_is_changed = False
        object_json = {}
        if os.path.exists(full_path):
            object_json = self.utils.readObjectJson(full_path)
            task = self.updateTask(task, object_json)
        else:
            logging.error("SkippedTask: %s - object config not found" % (task["NUMBER"],))
            return False

        try:
            assert len(task["ATTRIBUTES"]) > 0, logging.error(
                "'FIELDS' field of task with number %s is wrong" % (task["NUMBER"],))
        except AssertionError:
            logging.error("SkippedTask: %s - wrong task config" % (task["NUMBER"],))
            return False

        for item in task["ATTRIBUTES"].keys():
            if object_json[item] != task["ATTRIBUTES"][item]:
                object_json[item] = task["ATTRIBUTES"][item]
                config_is_changed = True

        if config_is_changed:
            self.utils.writeConfig(full_path, object_json)
        else:
            logging.error(
                "NoChanges: %s - config has not changed" % (task["NUMBER"],))

        return True

    def removeObject(self, task, full_path):
        config_is_changed = False
        object_json = {}
        if os.path.exists(full_path):
            object_json = self.utils.readObjectJson(full_path)
        else:
            logging.error("SkippedTask: %s - object config not found" % (task["NUMBER"],))
            return False

        if object_json["ENABLED"]:
            object_json["ENABLED"] = False
            config_is_changed = True

        if config_is_changed:
            self.utils.writeConfig(full_path, object_json)
        else:
            logging.error(
                "NoChanges: %s - config has not changed" % (task["NUMBER"],))

        return True

    def checkDataCast(self, task, full_path):
        config_builder = ConfigBuilder(self.config)

        object_json = {}
        if os.path.exists(full_path):
            object_json = self.utils.readObjectJson(full_path)
            task = self.updateTask(task, object_json)
        else:
            logging.error("SkippedTask: %s - object config not found" % (task["NUMBER"],))
            return False

        reader = self.defineReader(task)
        source_schema = reader.getSchema(task["SOURCE"])
        object_fields_names = {item["HIVE_NAME"]: item["DATA_TYPE"] for item in object_json["FIELDS"]}

        flag = True
        for item in object_fields_names.keys():
            schema_datatype = None
            try:
                schema_datatype = source_schema[item]["type"]
            except KeyError:
                logging.warning("FieldNotFound: Field '%s' for source schema of object '%s' at %s not found" % (
                    item, task["OBJECT"], task["SOURCE_DB"]))
                continue
            config_type = object_fields_names[item]
            generated_data_type = config_builder.getDataType(reader, None, item, source_schema, None)
            if config_type != generated_data_type:
                flag = False
                logging.warning(
                    "DataTypeMissCast: Config: '%s', Object: '%s', Field: '%s' - (config_type, generated_type, schema_type) = (%s, %s, %s)" % (
                        task["CONFIG"], task["OBJECT"], item, config_type, generated_data_type, schema_datatype))
        if flag:
            logging.info("Config datatypes are correct for : %s" % (task["OBJECT"],))

        return True

    def addFields(self, task, full_path):
        config_builder = ConfigBuilder(self.config)
        config_is_changed = False

        object_json = {}
        if os.path.exists(full_path):
            object_json = self.utils.readObjectJson(full_path)
            task = self.updateTask(task, object_json)
        else:
            logging.warning("Object config for object %s not found, path: %s" % (task["OBJECT"], full_path))
            try:
                assert "HIVE_TABLE_NAME" in task["ATTRIBUTES"].keys(), logging.error(
                    "'HIVE_TABLE_NAME' field of task with number %s is wrong" % (task["NUMBER"],))
                assert "TABLE_QUERY" in task["ATTRIBUTES"].keys(), logging.error(
                    "'TABLE_QUERY' field of task with number %s is wrong" % (task["NUMBER"],))
                assert "DESTINATION_TABLE_NAME" in task["ATTRIBUTES"].keys(), logging.error(
                    "'DESTINATION_TABLE_NAME' field of task with number %s is wrong" % (task["NUMBER"],))
                assert "SOURCE" in task.keys(), logging.error(
                    "'SOURCE' field of task with number %s is wrong" % (task["NUMBER"],))
            except AssertionError:
                logging.error("SkippedTask: %s - wrong task config" % (task["NUMBER"],))
                return False
            object_json = config_builder.createObjectFromTemplate(task)

        try:
            assert task["SOURCE_DB"] in ["ORACLE", "HEROKU", "SALESFORCE"], logging.error(
                "'SOURCE_DB' field of task with number %s is wrong" % (task["NUMBER"],))
            assert ("FIELDS" in task["ATTRIBUTES"] and len(task["ATTRIBUTES"]["FIELDS"]) > 0) or "ALL" in task[
                "ATTRIBUTES"], logging.error("'ATTRIBUTES' field of task with number %s is wrong" % (task["NUMBER"],))
        except AssertionError:
            logging.error("SkippedTask: %s - wrong task config" % (task["NUMBER"],))
            return False

        reader = self.defineReader(task)

        source_schema = reader.getSchema(task["SOURCE"])
        if not bool(source_schema):
            logging.error("SkippedTask: %s - source schema not found" % (task["NUMBER"],))
            return False

        fields_dict = {}

        if "ALL" in task["ATTRIBUTES"]["FIELDS"].keys() and task["ATTRIBUTES"]["FIELDS"]["ALL"]:
            fields_dict.update(self.verifyFields(self.getAllFields(source_schema), source_schema, object_json))
        else:
            fields_dict.update(self.verifyFields(task["ATTRIBUTES"]["FIELDS"], source_schema, object_json))

        if not bool(fields_dict):
            logging.error(
                "SkippedTask: %s - fields from task are already in config" % (task["NUMBER"],))
            return True

        fields_template = \
            self.utils.readTemplate(self.config["pwd"], task["CONFIG"].split("-")[0].lower())["FIELDS"][0]
        column_order = self.getMaxColumnOrder(object_json)
        for item in fields_dict.keys():
            column_order += 1
            fields_template_copy = copy.deepcopy(fields_template)
            for field_name in fields_template_copy:
                fields_template_copy[field_name] = config_builder.getFieldContent(reader,
                                                                                  task,
                                                                                  item,
                                                                                  field_name,
                                                                                  source_schema,
                                                                                  column_order)
            object_json["FIELDS"].append(fields_template_copy)
            config_is_changed = True

        if config_is_changed:
            self.utils.writeConfig(full_path, object_json)
        else:
            logging.error(
                "NoChanges: %s - config has not changed" % (task["NUMBER"],))

        return True

    def checkSalesForceApiName(self, task, full_path):
        object_json = {}
        if os.path.exists(full_path):
            object_json = self.utils.readObjectJson(full_path)
            task = self.updateTask(task, object_json)
        else:
            logging.error("SkippedTask: %s - object config not found" % (task["NUMBER"],))
            return False

        reader = self.defineReader(task)
        source_schema = reader.getSchema(task["SOURCE"])

        flag = True
        for item in object_json["FIELDS"]:
            schema_datatype = None
            if item["MISC"]["salesforceApiName"] not in source_schema.keys():
                logging.error("WrongSalesForceApiName: SalesForceApiName for field '%s' of object '%s' is wrong" % (
                    item["HIVE_NAME"], object_json["HIVE_TABLE_NAME"]))
                flag = False
        if flag:
            logging.info("Config datatypes are correct for : %s" % (item,))

        return True
