from simple_salesforce import Salesforce
from collections import OrderedDict
from readers.Reader import Reader
import logging


class SalesForceReader(Reader):
    def __init__(self,
                 config,
                 session=None,
                 attempts=None):
        self.session = Salesforce(username=config['SFDC_API_USER_NAME'],
                                  password=config['SFDC_API_PASSWORD'],
                                  security_token="",
                                  domain=config['SFDC_API_URL'].split("//")[1].split(".")[0])
        self.attempts = config['attempts']

    def getSchema(self, object_name):
        schema = {}
        attempt = 0
        baseURL = self.session.base_url.split('/services')[
                      0] + '/services/data/v42.0/sobjects/' + object_name + '/describe'

        while attempt < self.attempts:
            try:
                attempt += 1
                result = self.session._call_salesforce('GET', url=baseURL).json(object_pairs_hook=OrderedDict)
                break
            except Exception:
                print("Timed out while getting schema of object %s. Run next attempt." % (object_name,))

            if attempt == self.attempts:
                raise Exception("Timeout")

        for item in result['fields']:
            schema[item['name'].lower()] = {'field_name': item['name'],
                                            'type': item['type'],
                                            'length': item['length']}

        return schema

    def getDataType(self, source_type, length, field_name):
        type_cast = {
            'string': 'varchar(%s)' % (length * 4,),
            'date': 'timestamp',
            'double': 'float',
            'boolean': 'boolean',
            'datetime': 'timestamp',
            'currency': 'float',
            'reference': 'varchar(72)',
            'textarea': 'long varchar(65000)',
            'url': 'varchar(1000)',
            'email': 'varchar(400)',
            'picklist': 'varchar(1020)',
            'id': 'bigint',
            'phone': 'varchar(160)',
            'int': 'bigint',
            'multipicklist': 'varchar(16396)'
        }
        if source_type in type_cast:
            return type_cast[source_type]
        else:
            logging.error("DataTypeNotFound: Not such datatype: %s for field %s" % (source_type, field_name))
            raise Exception("Not such datatype: %s for field %s" % (source_type, field_name))
