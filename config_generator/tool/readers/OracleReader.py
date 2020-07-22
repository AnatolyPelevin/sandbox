import cx_Oracle


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

    def getSchema(self, object_name, schema_name):
        connection = cx_Oracle.makedsn(self.ip, self.port, self.sid)
        connect = cx_Oracle.connect(self.user, self.password, connection)
        cursor = connect.cursor()

        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH FROM all_tab_cols WHERE table_name='%s'" % (
                object_name,))

        schema = {}
        for row in cursor:
            schema[row[0].lower()] = {'field_name': row[0],
                                      'type': row[1],
                                      'length': row[2]}
        cursor.close()
        connect.close()
        return schema

    def getDataType(self, source_type, length, field_name):
        type_cast = {
            'NUMBER': 'bigint',
            'NVARCHAR2': 'varchar',
            'NVARCHAR2': 'varchar',
            'DATE': 'timestamp',
            'NUMBER': 'float',
            'TIMESTAMP(3) WITH LOCAL TIME ZONE': 'timestamp',
            'TIMESTAMP(6) WITH LOCAL TIME ZONE': 'timestamp',
            'TIMESTAMP(0) WITH LOCAL TIME ZONE': 'timestamp',
            'VARCHAR2': 'varchar',
            'VARCHAR2': 'varchar'
        }
        if source_type in type_cast:
            return type_cast[source_type]
        else:
            raise Exception("Not such datatype: %s for field %s" % (source_type, field_name))