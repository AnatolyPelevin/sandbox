from readers.OracleReader import OracleReader
from readers.SalesForceReader import SalesForceReader
from readers.HerokuReader import HerokuReader
from utils.Utils import Utils
from utils.ConfigBuilder import ConfigBuilder
from utils.TaskVerifier import TaskVerifier
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
            "CHECK DATATYPE CAST": self.checkDataCast,
            "CHECK SALESFORCE API NAME": self.checkSalesForceApiName
        }
        return tasks_dict[task_name](task, full_path)

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
        count = max(column_orders) if column_orders else 0
        return count

    def check_duplicates(self, object_config, fields_dict):
        object_config_fields_name = [item['HIVE_NAME'] for item in object_config['FIELDS']]
        checked_fields = {}
        for field in fields_dict:
            if field in object_config_fields_name:
                logging.warning(
                    "DuplicatedField: Field '{item}' for object '{hive_table_name}' is in config".format(item=field,
                                                                                                         hive_table_name=
                                                                                                         object_config[
                                                                                                             "HIVE_TABLE_NAME"]))
            else:
                checked_fields[field] = fields_dict[field]
        return checked_fields

    def verifyFields(self, fields_dict, source_schema, object_json):
        checked_fields_dict = {}
        for field_name in fields_dict:
            if field_name in source_schema:
                checked_fields_dict[field_name] = fields_dict[field_name]
            else:
                logging.error(
                    "FieldNotFound:No field '{item}' for object '{hive_table_name}' at source".format(item=field_name,
                                                                                                      hive_table_name=
                                                                                                      object_json[
                                                                                                          "HIVE_TABLE_NAME"]
                                                                                                      )
                )
        result = self.check_duplicates(object_json, checked_fields_dict)
        return result

    def updateTask(self, task, object_json):
        if "SOURCE" not in task.keys():
            if len(object_json["TABLE_QUERY"].split(".")) > 1:
                task["SOURCE"] = object_json["TABLE_QUERY"].split(".")[1]
            else:
                task["SOURCE"] = object_json["TABLE_QUERY"]
            logging.info(
                "Update 'SOURCE' field from object config for task with identifier: {id}".format(id=task["IDENTIFIER"]))

        return task

    def getAllFields(self, source_schema):
        result = {item: None for item in source_schema.keys()}
        return result

    def process(self, tasks):
        verifier = TaskVerifier()
        failed_tasks = []
        for task in tasks:
            verification_response = verifier.verifyTask(task)
            if verification_response:
                logging.info("Start task '{id}'".format(id=task["IDENTIFIER"]))
                full_path = "{config_path}/{config}/{path_in_config}/{env}/{object}.json.j2".format(
                    config_path=self.config["config_path"],
                    config=task['CONFIG'],
                    path_in_config='deployment/src/main/resources/include/json/config',
                    env=task["ENV"].lower(),
                    object=task["OBJECT"].lower()
                )

                result = self.defineTask(task, full_path)

                if result:
                    logging.info("SUCCESS: Task '{id}' was completed successfully \n".format(id=task["IDENTIFIER"]))
                else:
                    id = task["IDENTIFIER"]
                    logging.error("FAILED: Task '{id}' was completed unsuccessfully \n".format(id=id))
                    failed_tasks.append(id)
            else:
                logging.error("SkippedTask: '{id}' - wrong task config".format(id=task["IDENTIFIER"]))
                continue
        if failed_tasks:
            logging.error("Next tasks finished unsuccessfully: {failed}".format(failed=failed_tasks))


    def removeFields(self, task, full_path):
        config_is_changed = False
        object_json = {}
        if os.path.exists(full_path):
            object_json = Utils.readObjectJson(full_path)
        else:
            logging.error("SkippedTask: '{id}' - object config not found".format(id=task["IDENTIFIER"]))
            return False

        logging.info("Start task with id: '{id}'".format(id=task["IDENTIFIER"]))

        fields_dict = task["ATTRIBUTES"]["FIELDS"]
        for field in fields_dict.keys():
            order = [id for id, item in enumerate(object_json["FIELDS"]) if item["HIVE_NAME"] == field][0]
            if not object_json["FIELDS"][order]["IS_NULL"]:
                object_json["FIELDS"][order]["IS_NULL"] = True
                config_is_changed = True

        if config_is_changed:
            Utils.writeConfig(full_path, object_json)
        else:
            logging.error(
                "NoChanges: '{id}' - config has not changed".format(id=task["IDENTIFIER"]))
            return False

        return True

    def changeConfig(self, task, full_path):
        config_is_changed = False
        object_json = {}
        if os.path.exists(full_path):
            object_json = Utils.readObjectJson(full_path)
            task = self.updateTask(task, object_json)
        else:
            logging.error("SkippedTask: '{id}' - object config not found".format(id=task["IDENTIFIER"]))
            return False

        for item in task["ATTRIBUTES"].keys():
            if object_json[item] != task["ATTRIBUTES"][item]:
                object_json[item] = task["ATTRIBUTES"][item]
                config_is_changed = True

        if config_is_changed:
            Utils.writeConfig(full_path, object_json)
        else:
            logging.error(
                "NoChanges: '{id}' - config has not changed".format(id=task["IDENTIFIER"]))

        return True

    def removeObject(self, task, full_path):
        config_is_changed = False
        object_json = {}
        if os.path.exists(full_path):
            object_json = Utils.readObjectJson(full_path)
        else:
            logging.error("SkippedTask: '{id}' - object config not found".format(id=task["IDENTIFIER"]))
            return False

        if object_json["ENABLED"]:
            object_json["ENABLED"] = False
            config_is_changed = True

        if config_is_changed:
            Utils.writeConfig(full_path, object_json)
        else:
            logging.error(
                "NoChanges: '{id}' - config has not changed".format(id=task["IDENTIFIER"]))

        return True

    def checkDataCast(self, task, full_path):
        config_builder = ConfigBuilder(self.config)

        object_json = {}
        if os.path.exists(full_path):
            object_json = Utils.readObjectJson(full_path)
            task = self.updateTask(task, object_json)
        else:
            logging.error("SkippedTask: '{id}' - object config not found".format(id=task["IDENTIFIER"]))
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
                logging.warning(
                    "FieldNotFound: Field '{item}' for source schema of object '{object}' at {source_db} not found".format(
                        item=item,
                        object=task["OBJECT"],
                        source_db=task["SOURCE_DB"]
                    )
                )
                continue
            config_type = object_fields_names[item]
            generated_data_type = config_builder.getDataType(reader, None, item, source_schema, None)
            if config_type != generated_data_type:
                flag = False
                logging.warning(
                    "DataTypeMisCast: Config: '{config}', Object: '{object}', Field: '{item}' - (config_type, generated_type, schema_type) = ({config_type}, {generated_data_type}, {schema_datatype})".format(
                        config=task["CONFIG"],
                        object=task["OBJECT"],
                        item=item,
                        config_type=config_type,
                        generated_data_type=generated_data_type,
                        schema_datatype=schema_datatype
                    )
                )
        if flag:
            logging.info("Config datatypes are correct for : {object}".format(object=task["OBJECT"]))

        return True

    def addFields(self, task, full_path):
        config_builder = ConfigBuilder(self.config)
        verifier = TaskVerifier()
        config_is_changed = False

        object_json = {}
        if os.path.exists(full_path):
            object_json = Utils.readObjectJson(full_path)
            task = self.updateTask(task, object_json)
        else:
            logging.warning(
                "ObjectNotFound: Object config for object {object} not found, path: {full_path}".format(
                    object=task["OBJECT"],
                    full_path=full_path
                )
            )
            verification_response = verifier.verifyAddObject(task)
            if verification_response:
                object_json = config_builder.createObjectFromTemplate(task)
            else:
                logging.error("SkippedTask: '{id}' - wrong task config".format(id=task["IDENTIFIER"]))
                return False

        reader = self.defineReader(task)

        source_schema = reader.getSchema(task["SOURCE"])
        if not source_schema:
            logging.error("SkippedTask: '{id}' - source schema not found".format(id=task["IDENTIFIER"]))
            return False

        fields_dict = {}

        if "ALL" in task["ATTRIBUTES"]["FIELDS"].keys() and task["ATTRIBUTES"]["FIELDS"]["ALL"]:
            fields_dict.update(self.verifyFields(self.getAllFields(source_schema), source_schema, object_json))
        else:
            fields_dict.update(self.verifyFields(task["ATTRIBUTES"]["FIELDS"], source_schema, object_json))

        if not fields_dict:
            logging.error(
                "SkippedTask: '{id}' - fields from task are already in config".format(id=task["IDENTIFIER"]))
            return False

        fields_template = \
            Utils.readTemplate(self.config["pwd"], task["CONFIG"].split("-")[0].lower())["FIELDS"][0]
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
            Utils.writeConfig(full_path, object_json)
        else:
            logging.error(
                "NoChanges: '{id}' - config has not changed".format(id=task["IDENTIFIER"]))

        return True

    def checkSalesForceApiName(self, task, full_path):
        object_json = {}
        if os.path.exists(full_path):
            object_json = Utils.readObjectJson(full_path)
            task = self.updateTask(task, object_json)
        else:
            logging.error("SkippedTask: '{id}' - object config not found".format(id=task["IDENTIFIER"]))
            return False

        reader = self.defineReader(task)
        source_schema = reader.getSchema(task["SOURCE"])

        flag = True
        for item in object_json["FIELDS"]:
            if item["MISC"]["salesforceApiName"] not in source_schema.keys():
                logging.error(
                    "WrongSalesForceApiName: SalesForceApiName for field '{hive_name}' of object '{hive_table_name}' is wrong".format(
                        hive_name=item["HIVE_NAME"],
                        hive_table_name=object_json["HIVE_TABLE_NAME"]
                    )
                )
                flag = False
        if flag:
            logging.info("SalesForceApiName is correct for : {item}".format(item=task["OBJECT"]))
            return True

        return False
