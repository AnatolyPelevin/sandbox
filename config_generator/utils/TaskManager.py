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

    def defineTask(self, task):
        task_name = task["TASK"]
        tasks_dict = {
            "ADD FIELDS": self.addFields,
            "REMOVE FIELDS": self.removeFields,
            "CHANGE OBJECT CONFIG": self.changeConfig,
            "REMOVE OBJECT": self.removeObject,
            "CHECK DATATYPE CAST": self.checkDataCast,
            "CHECK SALESFORCE API NAME": self.checkSalesForceApiName,
            "CREATE CONFIG REPORT": self.createConfigReport
        }
        return tasks_dict[task_name](task)

    def defineReader(self, task):
        source_name = task["SOURCE_DB"]
        source_dict = {
            "ORACLE": OracleReader,
            "SALESFORCE": SalesForceReader,
            "HEROKU": HerokuReader
        }
        return source_dict[source_name](self.config)

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
                similar_names = Utils.find_field(source_schema, field_name)
                if similar_names:
                    logging.warning(
                        "FieldNotFound: No field '{item}' for object '{hive_table_name}' at source. Maybe you need some of this fields: {similar_fields_name}".format(
                            item=field_name,
                            hive_table_name=
                            object_json[
                                "HIVE_TABLE_NAME"],
                            similar_fields_name=similar_names
                        )
                    )
                else:
                    logging.error(
                        "FieldNotFound:No field '{item}' for object '{hive_table_name}' at source".format(
                            item=field_name,
                            hive_table_name=
                            object_json[
                                "HIVE_TABLE_NAME"]
                        )
                    )
        result = self.check_duplicates(object_json, checked_fields_dict)
        return result

    def updateTask(self, task, object_json):
        if "SOURCE" not in task:
            if len(object_json["TABLE_QUERY"].split(".")) > 1:
                task["SOURCE"] = object_json["TABLE_QUERY"].split(".")[1]
            else:
                task["SOURCE"] = object_json["TABLE_QUERY"]
            logging.info(
                "Update 'SOURCE' field from object config for task with identifier: {id}".format(id=task["IDENTIFIER"]))

        return task

    def getAllFields(self, source_schema):
        result = {key: None for key in source_schema}
        return result

    def process(self, tasks):
        verifier = TaskVerifier()
        failed_tasks = []
        for task in tasks:
            verification_response = verifier.verifyTask(task)
            if verification_response:
                logging.info("Start task '{id}'".format(id=task["IDENTIFIER"]))

                result = self.defineTask(task)

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

    def removeFields(self, task):
        config_is_changed = False
        object_json = {}
        full_path = Utils.get_path(self.config, task)
        if os.path.exists(full_path):
            object_json = Utils.readObjectJson(full_path)
        else:
            logging.error("SkippedTask: '{id}' - object config not found".format(id=task["IDENTIFIER"]))
            return False

        logging.info("Start task with id: '{id}'".format(id=task["IDENTIFIER"]))

        fields_dict = task["ATTRIBUTES"]["FIELDS"]
        for field in fields_dict:
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

    def changeConfig(self, task):
        config_is_changed = False
        object_json = {}
        full_path = Utils.get_path(self.config, task)
        if os.path.exists(full_path):
            object_json = Utils.readObjectJson(full_path)
            task = self.updateTask(task, object_json)
        else:
            logging.error("SkippedTask: '{id}' - object config not found".format(id=task["IDENTIFIER"]))
            return False

        for attribute in task["ATTRIBUTES"]:
            if object_json[attribute] != task["ATTRIBUTES"][attribute]:
                object_json[attribute] = task["ATTRIBUTES"][attribute]
                config_is_changed = True

        if config_is_changed:
            Utils.writeConfig(full_path, object_json)
        else:
            logging.error(
                "NoChanges: '{id}' - config has not changed".format(id=task["IDENTIFIER"]))

        return True

    def removeObject(self, task):
        config_is_changed = False
        object_json = {}
        full_path = Utils.get_path(self.config, task)
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

    def checkDataCast(self, task):
        config_builder = ConfigBuilder(self.config)

        object_json = {}
        full_path = Utils.get_path(self.config, task)
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
        for field in object_fields_names:
            schema_datatype = None
            try:
                schema_datatype = source_schema[field]["type"]
            except KeyError:
                logging.warning(
                    "FieldNotFound: Field '{item}' for source schema of object '{object}' at {source_db} not found".format(
                        item=field,
                        object=task["OBJECT"],
                        source_db=task["SOURCE_DB"]
                    )
                )
                continue
            config_type = object_fields_names[field]
            generated_data_type = config_builder.getDataType(reader, None, field, source_schema, None)
            if config_type != generated_data_type:
                flag = False
                logging.warning(
                    "DataTypeMisCast: Config: '{config}', Object: '{object}', Field: '{item}' - (config_type, generated_type, schema_type) = ({config_type}, {generated_data_type}, {schema_datatype})".format(
                        config=task["CONFIG"],
                        object=task["OBJECT"],
                        item=field,
                        config_type=config_type,
                        generated_data_type=generated_data_type,
                        schema_datatype=schema_datatype
                    )
                )
        if flag:
            logging.info("Config datatypes are correct for : {object}".format(object=task["OBJECT"]))

        return True

    def addFields(self, task):
        config_builder = ConfigBuilder(self.config)
        verifier = TaskVerifier()
        config_is_changed = False

        object_json = {}
        full_path = Utils.get_path(self.config, task)
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

        if "ALL" in task["ATTRIBUTES"]["FIELDS"] and task["ATTRIBUTES"]["FIELDS"]["ALL"]:
            fields_dict.update(self.verifyFields(self.getAllFields(source_schema), source_schema, object_json))
        else:
            fields_dict.update(self.verifyFields(task["ATTRIBUTES"]["FIELDS"], source_schema, object_json))

        if not fields_dict:
            logging.error(
                "SkippedTask: '{id}' - fields from task are already in config".format(id=task["IDENTIFIER"]))
            return False

        fields_template = \
            Utils.readTemplate(self.config["pwd"], task)["FIELDS"][0]
        column_order = self.getMaxColumnOrder(object_json)
        for field in fields_dict:
            column_order += 1
            fields_template_copy = copy.deepcopy(fields_template)
            for field_name in fields_template_copy:
                fields_template_copy[field_name] = config_builder.getFieldContent(reader,
                                                                                  task,
                                                                                  field,
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

    def checkSalesForceApiName(self, task):
        object_json = {}
        full_path = Utils.get_path(self.config, task)
        if os.path.exists(full_path):
            object_json = Utils.readObjectJson(full_path)
            task = self.updateTask(task, object_json)
        else:
            logging.error("SkippedTask: '{id}' - object config not found".format(id=task["IDENTIFIER"]))
            return False

        reader = self.defineReader(task)
        source_schema = reader.getSchema(task["SOURCE"])

        flag = True
        for field in object_json["FIELDS"]:
            if field["MISC"]["salesforceApiName"] not in source_schema:
                logging.error(
                    "WrongSalesForceApiName: SalesForceApiName for field '{hive_name}' of object '{hive_table_name}' is wrong".format(
                        hive_name=field["HIVE_NAME"],
                        hive_table_name=object_json["HIVE_TABLE_NAME"]
                    )
                )
                flag = False
        if flag:
            logging.info("SalesForceApiName is correct for : {item}".format(item=task["OBJECT"]))
            return True

        return False

    def createConfigReport(self, task):
        object_report_columns = ['OBJECT_NAME', 'ENV', 'PRIORITY', 'ENABLED', 'INGESTION_TYPE']
        fields_report_columns = ['OBJECT_NAME', 'ENV', 'COLUMN_ORDER','HIVE_NAME', 'SOURCE_NAME', 'ENABLED']
        supported_envs = task["ATTRIBUTES"]["REPORT_ENVS"]
        for env in supported_envs:
            object_report = []
            fields_report = []
            path_to_env_folder = Utils.get_config_path_with_env(self.config, task, env)
            all_object_at_env = Utils.get_all_object_by_path(path_to_env_folder)
            for object_name in all_object_at_env:
                full_path_to_object = Utils.get_path_to_object(self.config, task, env, object_name)
                object_json = Utils.readObjectJson(full_path_to_object)
                object_report.append({object_report_columns[0]: object_name,
                                      object_report_columns[1]: env,
                                      object_report_columns[2]: object_json[object_report_columns[2]],
                                      object_report_columns[3]: object_json[object_report_columns[3]],
                                      object_report_columns[4]: object_json[object_report_columns[4]]})
                for field in object_json["FIELDS"]:
                    fields_report.append({fields_report_columns[0]: object_name,
                                          fields_report_columns[1]: env,
                                          fields_report_columns[2]: field[fields_report_columns[2]],
                                          fields_report_columns[3]: field[fields_report_columns[3]],
                                          fields_report_columns[4]: field[fields_report_columns[4]],
                                          fields_report_columns[5]: not field["IS_NULL"]})
            Utils.write_csv_report(object_report, object_report_columns,
                                   "{config}_{env}_objects_report".format(config=task["CONFIG"], env=env))
            Utils.write_csv_report(fields_report, fields_report_columns,
                                   "{config}_{env}_fields_report".format(config=task["CONFIG"], env=env))

        return True
