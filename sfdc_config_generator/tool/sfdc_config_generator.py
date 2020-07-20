from simple_salesforce import Salesforce, SalesforceMalformedRequest
import os
from collections import OrderedDict
import pandas as pd
import json
import time
import signal
import psycopg2
from sshtunnel import SSHTunnelForwarder
from progress.bar import IncrementalBar
import datetime


class Utils:

    def __init__(self,
                 config,
                 dt=None,
                 report=None):
        self.report = {}
        self.config = config
        self.dt = str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))

    def read_changes_list(self, new_fields_file_name, new_formulas_fields_file_name):
        new_fields_file = open(new_fields_file_name, "r")
        new_fields = new_fields_file.readlines()
        new_fields_file.close()

        new_formulas_fields_file = open(new_formulas_fields_file_name, "r")
        new_formula_fields = new_formulas_fields_file.readlines()
        new_formulas_fields_file.close()

        changes = {}

        object_name = ""
        for item in new_fields:
            item = item.split("\n")[0]
            if item != "":
                if ":" in item:
                    object_name = item.split(":")[0].lower()
                if ":" not in item:
                    if object_name in changes.keys():
                        changes[object_name].update(self.getChangeItem(item.lower(), object_name, None))
                    else:
                        changes[object_name] = self.getChangeItem(item.lower(), object_name, None)

        formula_ingestion = "FORMULA"
        for item in new_formula_fields:
            item = item.split("\n")[0]
            if item != "":
                if ":" in item:
                    object_name = item.split(":")[0].lower()
                if ":" not in item:
                    if object_name in changes.keys():
                        changes[object_name].update(self.getChangeItem(item.lower(), object_name, formula_ingestion))
                    else:
                        changes[object_name] = self.getChangeItem(item.lower(), object_name, formula_ingestion)
        return changes

    def getChangeItem(self, field_name, object_name, ingestion_type):
        return {object_name + "." + field_name: {'HIVE_NAME': None,
                                                 'DATA_TYPE': None,
                                                 'COLUMN_ORDER': None,
                                                 'IS_NULL': False,
                                                 'INGESTION_TYPE': ingestion_type,
                                                 'salesforceApiName': None,
                                                 'source_datatype': None,
                                                 'source_length': None,
                                                 'exist_on_PROD': False,
                                                 'exist_on_LAB': False,
                                                 'in_config': False,
                                                 'message': ""}}

    def signal_handler(self, signum, frame):
        raise Exception("Timed out!")

    def find_field(self, schema, field_name):
        similar_names = []
        for schema_field in schema.keys():
            distance = self.get_distance(schema_field, field_name)
            similar_names.append((schema[schema_field]['name'], distance))
        return sorted(similar_names, key=lambda x: x[1])[:3]

    def get_distance(self, a, b):
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

    def check_duplicates(self, object_config, changes):
        object_config_fields_name = {item['HIVE_NAME']: item['COLUMN_ORDER'] for item in object_config['FIELDS']}
        for item in changes:
            hive_name = changes[item]['HIVE_NAME']
            if hive_name in object_config_fields_name.keys():
                changes[item]['COLUMN_ORDER'] = object_config_fields_name[hive_name]
                changes[item]['in_config'] = True

        return changes

    def write_config(self, object_config, changes):
        column_orders = [item['COLUMN_ORDER'] for item in object_config['FIELDS']]

        count = 0 if not column_orders else max([item['COLUMN_ORDER'] for item in object_config['FIELDS']])
        for item in changes:
            field_config = changes[item]
            condition = (field_config['exist_on_PROD'] | (
                    field_config['INGESTION_TYPE'] == "FORMULA")) & (not field_config['in_config'])
            if condition:
                count += 1
                self.report[item]['COLUMN_ORDER'] = int(count)
                self.report[item]['in_config'] = True
                config_block = Field(field_config['HIVE_NAME'],
                                     field_config['HIVE_NAME'],
                                     field_config['HIVE_NAME'],
                                     field_config['DATA_TYPE'],
                                     count,
                                     field_config['IS_NULL'],
                                     field_config['INGESTION_TYPE'],
                                     field_config['salesforceApiName']).get_config_block_dict()
                object_config['FIELDS'].append(config_block)
        return object_config

    def write_log(self, msg):
        time = str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
        with open("logs/" + self.dt + '.log', 'a+') as outfile:
            outfile.write(time + " | " + msg + "\n")


class Field:

    def __init__(self,
                 hive_name,
                 source_name,
                 destination_name,
                 data_type,
                 column_order,
                 is_null,
                 ingestion_type,
                 salesforce_api_name):
        self.hive_name = hive_name
        self.source_name = source_name
        self.destination_name = destination_name
        self.data_type = data_type
        self.column_order = column_order
        self.is_null = is_null
        self.ingestion_type = ingestion_type
        self.salesforce_api_name = salesforce_api_name

    def get_config_block_dict(self):
        return {"HIVE_NAME": self.hive_name,
                "SOURCE_NAME": self.source_name,
                "DESTINATION_NAME": self.destination_name,
                "DATA_TYPE": self.data_type,
                "COLUMN_ORDER": self.column_order,
                "IS_NULL": bool(self.is_null),
                "INGESTION_TYPE": self.ingestion_type,
                "MISC": {"salesforceApiName": self.salesforce_api_name}}


class ConfigTemplate:

    def __init__(self,
                 hive_table_name):
        self.hive_table_name = hive_table_name

    def get_jdbc_object(self):
        object_conf = {
            "HIVE_TABLE_NAME": self.hive_table_name,
            "TABLE_QUERY": "salesforce.%s" % (self.hive_table_name,),
            "DESTINATION_TABLE_NAME": self.hive_table_name,
            "IS_PARTITIONED": True,
            "PRIORITY": 10000,
            "ENABLED": True,
            "INGESTION_TYPE": "JDBC",
            "MISC": {
                "chunkedExport": {
                    "chunkByColumn": "id",
                    "numberOfChunks": 40
                },
                "addInsertTimestamp": False,
                "rotateOldPartitions": False,
                "makeTimestampsImpalaCompatible": False,
                "onChangeExport": False
            },
            "FIELDS": []
        }
        return object_conf

    def get_api_object(self):
        object_conf = {
            "HIVE_TABLE_NAME": self.hive_table_name,
            "TABLE_QUERY": self.hive_table_name,
            "DESTINATION_TABLE_NAME": self.hive_table_name,
            "IS_PARTITIONED": True,
            "PRIORITY": 10000,
            "ENABLED": True,
            "INGESTION_TYPE": "API",
            "MISC": {
                "addInsertTimestamp": False,
                "rotateOldPartitions": False,
                "makeTimestampsImpalaCompatible": False,
                "apiFullExport": {
                    "fullExportFrequency": 24
                },
                "apiIncrementalExport": {
                    "incrementByField": "systemmodstamp",
                    "incrementFieldFormat": "yyyy-MM-dd'T'HH:mm:ss.F'Z'",
                    "incrementFrequency": 0
                },
                "onChangeExport": False,
                "apiLoadingType": "pkChunkingLoading"
            },
            "FIELDS": []
        }
        return object_conf


class Generator:

    def __init__(self, config, utils, object_changes_list):
        self.utils = utils
        self.config = config
        self.object_changes_list = object_changes_list

    def set_minor_sf_info(self, sf_schema):
        for item in self.object_changes_list:
            field_name = item.split(".")[1]
            flag = field_name in sf_schema.keys()
            if flag:
                self.object_changes_list[item]['salesforceApiName'] = sf_schema[field_name]['name'].lower()

    def set_ingestion_type(self, ingestion_type):
        for item in self.object_changes_list:
            if self.object_changes_list[item]['INGESTION_TYPE'] is None:
                self.object_changes_list[item]['INGESTION_TYPE'] = ingestion_type

    def set_source_prod_info(self, schema):
        for item in self.object_changes_list:
            field_name = item.split(".")[1]
            if self.object_changes_list[item]['INGESTION_TYPE'] != "FORMULA":
                flag = field_name in schema.keys()
                if flag:
                    source_length = int(schema[field_name]['length']) if schema[field_name]['length'] != None else 0
                    self.object_changes_list[item]['source_datatype'] = schema[field_name]['type']
                    self.object_changes_list[item]['source_length'] = source_length
                    self.object_changes_list[item]['exist_on_PROD'] = flag

                else:
                    self.object_changes_list[item]['exist_on_PROD'] = flag
                    similar_names = self.utils.find_field(schema, field_name)
                    self.object_changes_list[item]['message'] = "" if not similar_names else similar_names

    def set_source_lab_info(self, schema):
        for item in self.object_changes_list:
            field_name = item.split(".")[1]
            flag = field_name in schema.keys()
            if flag:
                self.object_changes_list[item]['exist_on_LAB'] = flag
            else:
                self.object_changes_list[item]['exist_on_LAB'] = flag

    def set_config_fields(self, source_utils):
        for item in self.object_changes_list:
            datatype = None
            if self.object_changes_list[item]['INGESTION_TYPE'] != "FORMULA" and self.object_changes_list[item][
                'exist_on_PROD'] == True:
                datatype = source_utils.get_datatype(self.object_changes_list[item]['source_datatype'],
                                                     self.object_changes_list[item]['source_length'],
                                                     self.object_changes_list[item]['salesforceApiName'])
            self.object_changes_list[item]['HIVE_NAME'] = field_name = item.split(".")[1]
            self.object_changes_list[item]['DATA_TYPE'] = datatype

    def process(self, object_name):
        object_config = None
        self.utils.write_log("Process fields for object %s" % (object_name,))
        path_to_file = self.config["path"] + '/' + object_name + '.json.j2'

        if os.path.exists(path_to_file):
            with open(path_to_file) as json_file:
                object_config = json.load(json_file)
        else:
            object_config = ConfigTemplate(object_name).get_jdbc_object()

        sf_utils = SF_Utils(self.config, self.utils)
        pg_utils = PG_Utils(self.config, self.utils)

        if object_config['INGESTION_TYPE'] == "API":
            sf_prod_schema = sf_utils.get_schema(object_name, "PROD")
            sf_lab_schema = sf_utils.get_schema(object_name, "LAB")

            self.set_ingestion_type("API")
            self.set_source_prod_info(sf_prod_schema)
            self.set_source_lab_info(sf_lab_schema)
            self.set_minor_sf_info(sf_prod_schema)
            self.set_config_fields(sf_utils)

        if object_config['INGESTION_TYPE'] == "JDBC":
            pg_prod_schema = pg_utils.get_schema(object_name, "PROD")
            sf_prod_schema = sf_utils.get_schema(object_name, "PROD")

            self.set_ingestion_type("JDBC")
            self.set_source_prod_info(pg_prod_schema)
            self.set_source_lab_info(pg_prod_schema)
            self.set_minor_sf_info(sf_prod_schema)
            self.set_config_fields(pg_utils)

        self.object_changes_list = self.utils.check_duplicates(object_config, self.object_changes_list)

        self.utils.report.update(self.object_changes_list)
        if config['change_object_config']:
            utils.write_log("Edit config for object %s." % (object_name,))
            with open(path_to_file, 'w') as outfile:
                object_config_updated = utils.write_config(object_config, self.object_changes_list)
                json.dump(object_config_updated, outfile, indent=4)


class SF_Utils:

    def __init__(self,
                 config,
                 utils,
                 sf_prod_session=None,
                 sf_lab_session=None,
                 attempts=None):
        self.sf_prod_session = Salesforce(username=config['sf_prod_username'],
                                          password=config['sf_prod_password'],
                                          security_token=config['sf_prod_security_token'],
                                          domain=config['sf_prod_domain'])
        self.sf_lab_session = Salesforce(username=config['sf_lab_username'],
                                         password=config['sf_lab_password'],
                                         security_token=config['sf_lab_security_token'],
                                         domain=config['sf_lab_domain'])
        self.attempts = config['attempts']
        self.create_schema_files = config['create_schema_files']
        self.utils = utils

    def get_schema(self, object_name, env):
        schema = {}
        result = ""
        attempt = 0
        session = self.sf_prod_session if env == "PROD" else self.sf_lab_session
        baseURL = session.base_url.split('/services')[0] + '/services/data/v42.0/sobjects/' + object_name + '/describe'

        while attempt < self.attempts:
            try:
                attempt += 1
                self.utils.write_log("Getting schema of object %s on %s. Attempt: %s" % (object_name, env, attempt))
                result = session._call_salesforce('GET', url=baseURL).json(object_pairs_hook=OrderedDict)
                break
            except Exception:
                utils.write_log(
                    "Timed out while getting schema of object %s on %s. Run next attempt." % (object_name, env))

            if attempt == self.attempts:
                raise Exception("Timeout")

        for item in result['fields']:
            schema[item['name'].lower()] = {'name': item['name'], 'type': item['type'], 'length': item['length']}

        if (env == "PROD") & self.create_schema_files:
            with open(config['out_path'] + "/" + object_name + "_schema.json", 'w') as outfile:
                json.dump(schema, outfile, indent=4)

        return schema

    def get_datatype(self, sf_type, length, name):
        type_cast = {
            'string': 'varchar(%s)' % (length * 4,),
            'date': 'timestamp',
            'double': 'float',
            'boolean': 'boolean',
            'datetime': 'timestamp',
            'currency': 'float',
            'reference': 'varchar(72)',
            'textarea': 'varchar(1020)',
            'url': 'varchar(1000)',
            'email': 'varchar(400)',
            'picklist': 'varchar(10)'
        }
        if sf_type in type_cast:
            return type_cast[sf_type]
        else:
            raise Exception("Not such datatype: %s for field %s" % (sf_type, name))


class PG_Utils:

    def __init__(self,
                 config,
                 utils,
                 pg_ssh_tunnel=None,
                 pg_prod_connection=None,
                 attempts=None):
        self.pg_ssh_tunnel = SSHTunnelForwarder((config['ssh_host'], 22),
                                                ssh_username=config['user'],
                                                ssh_password=config['password'],
                                                remote_bind_address=(config['pg_host'], 5432)
                                                )

        self.attempts = config['attempts']
        self.create_schema_files = config['create_schema_files']
        self.config = config
        self.utils = utils

    def get_schema(self, object_name, env):
        self.utils.write_log("Getting schema of object %s on %s." % (object_name, env))
        self.pg_ssh_tunnel.start()
        conn = psycopg2.connect(
            database=config['pg_database'],
            user=config['pg_user'],
            password=config['pg_password'],
            host=self.pg_ssh_tunnel.local_bind_host,
            port=self.pg_ssh_tunnel.local_bind_port,
        )

        cur = conn.cursor()

        query = """SELECT column_name, data_type, character_maximum_length
                   FROM information_schema.columns
                   WHERE table_schema = 'salesforce'
                   AND table_name   = '%s';
                """ % (object_name,)

        cur.execute(query)
        result = cur.fetchall()

        conn.close()
        self.pg_ssh_tunnel.stop()

        schema = {}
        for item in result:
            schema[item[0].lower()] = {'name': item[0],
                                       'type': item[1],
                                       'length': item[2]}

        if (env == "PROD") & self.create_schema_files:
            with open(config['out_path'] + "/" + object_name + "_schema.json", 'w') as outfile:
                json.dump(schema, outfile, indent=4)

        return schema

    def get_datatype(self, source_type, length, name):
        length = length if length != None else 0
        type_cast = {
            'character varying': 'varchar(%s)' % (length * 4,),
            'double precision': 'float',
            'timestamp without time zone': 'timestamp',
            'date': 'timestamp',
            'text': 'long varchar(65000)',
            'integer': 'bigint',
            'boolean': 'boolean'
        }
        if source_type in type_cast:
            return type_cast[source_type]
        else:
            raise Exception("Not such datatype: '%s' for field '%s'" % (source_type, name))


if __name__ == '__main__':
    config = {}

    with open('config.json') as json_file:
        config = json.load(json_file)

    with open('tool/passwords.json') as json_file:
        config.update(json.load(json_file))

    utils = Utils(config)

    new_fields_list = utils.read_changes_list(config['fields_file_name'], config['formula_fields_file_name'])

    bar = IncrementalBar('Progress', max=len(new_fields_list.keys()))

    signal.signal(signal.SIGALRM, utils.signal_handler)
    signal.alarm(10)

    for object_name in new_fields_list.keys():
        utils.write_log("Process fields for object %s" % (object_name,))
        Generator(config, utils, new_fields_list[object_name]).process(object_name)
        bar.next()

    report_columns = [item for item in config['report_columns'] if config['report_columns'][item] == True]
    df = pd.DataFrame.from_dict(utils.report, orient='index', columns=report_columns)
    df.to_csv(config['out_path'] + config['report_file_name'] + "_" + utils.dt + ".csv", encoding='utf-8',
              na_rep='None')
    bar.finish()
