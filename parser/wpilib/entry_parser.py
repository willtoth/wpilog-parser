from datalog import StartRecordData, DataLogRecord
from typing import Callable, Any, Dict

def struct_parser(schema: str) -> Callable[[DataLogRecord], Any]:
    fields = schema.split(";")
    print(fields)

    def result(record):
        return None
    return result

class EntryParser:
    def __init__(self):
        self._types: Dict[str, Callable[[DataLogRecord], Any]] = {
            "double": lambda record: record.getDouble(),
            "int64": lambda record: record.getInteger(),
            "string": lambda record: record.getString(),
            "json": lambda record: record.getString(),
            "boolean": lambda record: record.getBoolean(),
            "boolean[]": lambda record: record.getBooleanArray(),
            "double[]": lambda record: record.getDoubleArray(),
            "float[]": lambda record: record.getFloatArray(),
            "int64[]": lambda record: record.getIntegerArray(),
            "string[]": lambda record: record.getStringArray(),
            "msgpack": lambda record: record.getMsgPack(),
        }

    def __empty_praser(record: DataLogRecord) -> None:
        return None

    def types(self) -> list[str]:
        """Get a list of all registered type names
        
        Returns:
            List of type names that have registered processors
        """
        return list(self._types.keys())

    def register_type(self, name: str):
        """Register a data type from datalog entry

        Args:
            entry_name: Raw entry name (e.g. '/.schema/struct:Translation3d')
            schema_data: Schema definition (e.g. 'double x;double y;double z')
        """
        print(f"TYPE: {name}")
        self._types[name] = self.__empty_praser
        
        # Parse field definitions
        # fields = []
        # for field in schema_data.split(';'):
        #     if not field:
        #         continue
        #     field_type, field_name = field.strip().split(' ')
        #     fields.append({
        #         'name': field_name,
        #         'type': field_type
        #     })
            
        # # Store schema definition
        # self.schemas[schema_name] = {
        #     'name': schema_name,
        #     'fields': fields
        # }
    
    def define_parser(self, name: str, parser: Callable[[DataLogRecord], Any]):
        self._types[name] = parser
