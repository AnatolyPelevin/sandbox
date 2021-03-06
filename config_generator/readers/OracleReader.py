import cx_Oracle
from readers.Reader import Reader
import logging


class OracleReader(Reader):
    def __init__(self,
                 config,
                 user=None,
                 password=None,
                 ip=None,
                 port=None,
                 sid=None):
        super().__init__(config)
        self.user = self.config['DWH_ORACLE_ERD_USER_NAME']
        self.password = self.config['DWH_ORACLE_ERD_PASSWORD']
        self.ip, self.port, self.sid = self.config['DWH_ORACLE_DATA_SOURCE_URL'].split("@")[1].split(":")

    def getSchema(self, object_name):
        connection = cx_Oracle.makedsn(self.ip, self.port, self.sid)
        connect = cx_Oracle.connect(self.user, self.password, connection)
        cursor = connect.cursor()

        query = """
                    SELECT column_name,
                           data_type,
                           data_length
                      FROM all_tab_cols
                     WHERE table_name='{object_name}'
                """.format(object_name=object_name.upper())

        logging.info("Source schema query: {query}".format(query=query))

        cursor.execute(query)

        schema = {field_name.lower(): {'field_name': field_name, 'type': type, 'length': length}
          for (field_name, type, length) in cursor}

        cursor.close()
        connect.close()
        return schema

    def getDataType(self, source_type, length, field_name):
        type_cast = {
            'NUMBER': 'bigint',
            'NVARCHAR2': 'varchar',
            'DATE': 'timestamp',
            'TIMESTAMP(3) WITH LOCAL TIME ZONE': 'timestamp',
            'TIMESTAMP(6) WITH LOCAL TIME ZONE': 'timestamp',
            'TIMESTAMP(0) WITH LOCAL TIME ZONE': 'timestamp',
            'CHAR': 'varchar',
            'VARCHAR2': 'varchar'
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
