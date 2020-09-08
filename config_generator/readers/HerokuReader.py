from sshtunnel import SSHTunnelForwarder
import psycopg2
from readers.Reader import Reader
import logging


class HerokuReader(Reader):
    def __init__(self,
                 config,
                 ssh_tunel=None):
        self.ssh_tunnel = SSHTunnelForwarder((config['ssh_host'], 22),
                                             ssh_username=config['user'],
                                             ssh_password=config['password'],
                                             remote_bind_address=(config['SFDC_HEROKU_POSTGRES_HOST'], 5432)
                                             )
        self.config = config

    def getSchema(self, object_name):
        self.ssh_tunnel.start()
        conn = psycopg2.connect(database=self.config['SFDC_HEROKU_POSTGRES_DB_NAME'],
                                user=self.config['SFDC_HEROKU_POSTGRES_USER'],
                                password=self.config['SFDC_HEROKU_POSTGRES_PASSWORD'],
                                host=self.ssh_tunnel.local_bind_host,
                                port=self.ssh_tunnel.local_bind_port,
                                )
        cur = conn.cursor()

        query = """
                    SELECT column_name, 
                           data_type, 
                           character_maximum_length
                      FROM information_schema.columns 
                     WHERE table_schema = 'salesforce' 
                       AND table_name   = '{obj_name}';
                """.format(obj_name=object_name.lower().replace('\"', ""))

        logging.info("Source schema query: {query}".format(query=query))

        cur.execute(query)
        result = cur.fetchall()

        conn.close()
        self.ssh_tunnel.stop()

        schema = {field_name.lower(): {'field_name': field_name, 'type': type, 'length': length}
                  for (field_name, type, length) in result}

        return schema

    def getDataType(self, source_type, length, name):
        length = length if length else 0
        type_cast = {
            'character varying': 'varchar({type_length})'.format(type_length=length * 4),
            'double precision': 'float',
            'timestamp without time zone': 'timestamp',
            'date': 'timestamp',
            'text': 'long varchar(65000)',
            'integer': 'bigint',
            'boolean': 'boolean'
        }
        try:
            return type_cast[source_type]
        except KeyError:
            logging.error(
                "DataTypeNotFound: Not such datatype: {source_type} for field {field_name}".format(
                    source_type=source_type,
                    field_name=name
                )
            )
            raise
