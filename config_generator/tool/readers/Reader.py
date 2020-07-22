class Reader:

    def __init__(self, config):
        self.config = config

    def getSchema(self, object_name, schema_name):
        raise NotImplementedError

    def getDataType(self, source_type, length, field_name):
        raise NotImplementedError