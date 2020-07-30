from simple_salesforce import Salesforce, SFType
from collections import OrderedDict
from readers.Reader import Reader
import multiprocessing as mp
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
        attempt = 0
        result = {}
        while attempt < self.attempts:
            attempt += 1
            ctx = mp.get_context('spawn')
            queue = ctx.Queue()
            process = mp.Process(target=self.callSalesforce(object_name, queue))
            process.start()
            process.join(20)
            if process.is_alive():
                if attempt == self.attempts:
                    logging.error("SalesForce connection Timeout")
                    raise TimeoutError
                else:
                    logging.error("Timed out while getting schema of object {object_name}. Run next attempt.".format(
                        object_name=object_name))
                    process.kill()
                    process.close()
            else:
                result = queue.get()
                break

        schema = {item['name'].lower(): {'field_name': item['name'], 'type': item['type'], 'length': item['length']}
                  for item in result['fields']}

        return schema

    def callSalesforce(self, object_name, q):
        sf_type = SFType(object_name,self.session.session_id,self.session.sf_instance)
        result = sf_type.describe()
        q.put(result)

    def getDataType(self, source_type, length, field_name):
        type_cast = {
            'string': 'varchar({type_length})'.format(type_length=length * 4),
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
        try:
            return type_cast[source_type]
        except KeyError:
            logging.error(
                "DataTypeNotFound: Not such datatype: {source_type} for field {field_name}".format(
                    source_type=source_type,
                    field_name=field_name
                )
            )
            raise
