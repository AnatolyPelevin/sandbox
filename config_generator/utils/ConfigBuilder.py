from utils.Utils import Utils
import logging


class ConfigBuilder:

    def __init__(self,
                 config):
        self.config = config

    ingestion_types_dict = {
        "SALESFORCE": "API",
        "HEROKU": "JDBC"
    }

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
        if task["SOURCE_DB"] in self.ingestion_types_dict.keys():
            return self.ingestion_types_dict[task["SOURCE_DB"]]
        else:
            raise NotImplementedError

    def getMisc(self, reader, task, column_name, source_schema, column_order):
        misc_dict = {
            "sfdc-config": {"salesforceApiName": column_name},
            "erd-configs": {}
        }
        if task["CONFIG"] in misc_dict.keys():
            return misc_dict[task["CONFIG"]]
        else:
            raise NotImplementedError

    def createObjectFromTemplate(self, task):
        utils = Utils()

        object_json = utils.readTemplate(self.config["pwd"], task["CONFIG"].split("-")[0].lower())
        object_json["HIVE_TABLE_NAME"] = task["ATTRIBUTES"]["HIVE_TABLE_NAME"]
        object_json["DESTINATION_TABLE_NAME"] = task["ATTRIBUTES"]["DESTINATION_TABLE_NAME"]
        object_json["FIELDS"] = []

        if task["CONFIG"] == "sfdc-config":
            object_json["INGESTION_TYPE"] = self.ingestion_types_dict[task["SOURCE_DB"]]

        if task["CONFIG"] == "erd-configs":
            object_json["TABLE_QUERY"] = task["ATTRIBUTES"]["TABLE_QUERY"].upper()
        else:
            object_json["TABLE_QUERY"] = task["ATTRIBUTES"]["TABLE_QUERY"].lower()

        logging.info("Config from template for object %s was created" % (task["OBJECT"],))

        return object_json
