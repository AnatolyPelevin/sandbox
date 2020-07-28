import logging


class TaskVerifier:

    def __defineVerifier(self, task):
        types_of_tasks = {
            "ADD FIELDS": self._verifyAddFields,
            "REMOVE FIELDS": self._verifyRemoveFields,
            "CHANGE OBJECT CONFIG": self._verifyChangeConfig,
            "REMOVE OBJECT": self._verifyRemoveObject,
            "CHECK DATATYPE CAST": self._verifyCheckDataCast,
            "CHECK SALESFORCE API NAME": self._verifCheckSalesForceApiName
        }
        return types_of_tasks[task["TASK"]](task)

    def verifyTask(self, task):
        flag = True

        if task["TASK"] is None:
            logging.error("'TASK' field of task with number {number} is wrong".format(number=task["NUMBER"]))
            flag = False
        if task["CONFIG"] is None or task["CONFIG"] not in ["erd-configs", "sfdc-config"]:
            logging.error("'CONFIG' field of task with number {number} is wrong".format(number=task["NUMBER"]))
            flag = False
        if task["OBJECT"] is None:
            logging.error("'OBJECT' field of task with number {number} is wrong".format(number=task["NUMBER"]))
            flag = False
        if task["ENV"] is None or task["ENV"] not in ["RC", "ATT", "UAT"]:
            logging.error("'ENV' field of task with number {number} is wrong".format(number=task["NUMBER"]))
            flag = False

        flag = flag and self.__defineVerifier(task)

        return flag

    @staticmethod
    def _verifyAddFields(task):
        flag = True
        if task["SOURCE_DB"] not in ["ORACLE", "HEROKU", "SALESFORCE"]:
            logging.error("'SOURCE_DB' field of task with number {number} is wrong".format(number=task["NUMBER"]))
            flag = False
        if ("FIELDS" not in task["ATTRIBUTES"] or len(task["ATTRIBUTES"]["FIELDS"]) == 0) and "ALL" not in task[
            "ATTRIBUTES"]:
            logging.error("'ATTRIBUTES' field of task with number {number} is wrong".format(number=task["NUMBER"]))
            flag = False
        return flag

    @staticmethod
    def verifyAddObject(task):
        flag = True
        if "HIVE_TABLE_NAME" not in task["ATTRIBUTES"].keys():
            logging.error("'HIVE_TABLE_NAME' field of task with number {number} is wrong".format(number=task["NUMBER"]))
            flag = False
        if "TABLE_QUERY" not in task["ATTRIBUTES"].keys():
            logging.error("'TABLE_QUERY' field of task with number {number} is wrong".format(number=task["NUMBER"]))
            flag = False
        if "DESTINATION_TABLE_NAME" not in task["ATTRIBUTES"].keys():
            logging.error(
                "'DESTINATION_TABLE_NAME' field of task with number {number} is wrong".format(number=task["NUMBER"]))
            flag = False
        if "SOURCE" not in task.keys():
            logging.error("'SOURCE' field of task with number {number} is wrong".format(number=task["NUMBER"]))
            flag = False
        return flag

    @staticmethod
    def _verifyRemoveFields(task):
        flag = True
        if "FIELDS" not in task["ATTRIBUTES"] or len(task["ATTRIBUTES"]["FIELDS"]) == 0:
            logging.error("'FIELDS' field of task with number {number} is wrong".format(number=task["NUMBER"]))
            flag = False
        return flag

    @staticmethod
    def _verifyChangeConfig(task):
        flag = True
        if len(task["ATTRIBUTES"]) == 0:
            logging.error("'FIELDS' field of task with number {number} is wrong".format(number=task["NUMBER"]))
            flag = False
        return flag

    @staticmethod
    def _verifyRemoveObject(task):
        return True

    @staticmethod
    def _verifyCheckDataCast(task):
        return True

    @staticmethod
    def _verifCheckSalesForceApiName(task):
        return True
