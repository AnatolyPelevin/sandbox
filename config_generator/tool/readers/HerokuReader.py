from sshtunnel import SSHTunnelForwarder
import psycopg2
import Reader


class HerokuReader(Reader):
    def __init__(self,
                 config,
                 ssh_tunel=None):
        self.ssh_tunel = SSHTunnelForwarder((config['ssh_host'], 22),
                                            ssh_username=config['user'],
                                            ssh_password=config['password'],
                                            remote_bind_address=(config['SFDC_HEROKU_POSTGRES_HOST'], 5432)
                                            )
        self.config = config

    def getSchema(self, object_name, schema_name):
        self.ssh_tunnel.start()
        conn = psycopg2.connect(database=self.config['SFDC_HEROKU_POSTGRES_DB_NAME'],
                                user=self.config['SFDC_HEROKU_POSTGRES_USER'],
                                password=self.config['SFDC_HEROKU_POSTGRES_PASSWORD'],
                                host=self.ssh_tunnel.local_bind_host,
                                port=self.ssh_tunnel.local_bind_port,
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
            schema[item[0].lower()] = {'field_name': item[0],
                                       'type': item[1],
                                       'length': item[2]}
        return schema

    def getDataType(self, source_type, length, name):
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
