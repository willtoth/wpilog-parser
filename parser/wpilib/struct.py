import re
import struct

def compile_struct_parser(schema_str):
    """
    Given a schema string defining a packed structure (using C-like syntax as specified),
    returns a parser function that takes a bytes object and returns a dict mapping field
    names to parsed values.
    """
    # Regular expressions for declarations:
    # Standard declaration: optional enum, then type, identifier, optional array size.
    std_regex = re.compile(
        r'^(?:enum\s*\{[^}]*\}\s*)?'    # optional enum spec
        r'(\w+)\s+'                     # type name
        r'(\w+)'                        # field name
        r'(?:\s*\[\s*(\d+)\s*\])?'       # optional array size
        r'\s*$'
    )
    # Bit-field declaration: optional enum, then type, identifier, colon, bit width.
    bit_regex = re.compile(
        r'^(?:enum\s*\{[^}]*\}\s*)?'
        r'(\w+)\s+'
        r'(\w+)\s*:\s*'
        r'(\d+)\s*$'
    )

    # First, split the schema string by semicolons and parse each declaration.
    declarations = [decl.strip() for decl in schema_str.split(';') if decl.strip()]
    field_descriptors = []
    for decl in declarations:
        m = bit_regex.match(decl)
        if m:
            type_name, field_name, bits = m.groups()
            field_descriptors.append({
                'name': field_name,
                'type': type_name,
                'bit_field': True,
                'bits': int(bits)
            })
            continue
        m = std_regex.match(decl)
        if m:
            type_name, field_name, array_size = m.groups()
            descriptor = {
                'name': field_name,
                'type': type_name,
                'bit_field': False,
            }
            if array_size:
                descriptor['array'] = int(array_size)
            field_descriptors.append(descriptor)
            continue
        raise ValueError(f"Unable to parse declaration: {decl}")

    # Next, we compile the field list into a sequence of operations.
    # We want to treat standard fields separately from bit-field groups.
    ops = []
    i = 0
    while i < len(field_descriptors):
        desc = field_descriptors[i]
        if not desc['bit_field']:
            # Standard field op.
            ops.append({'op': 'field', 'desc': desc})
            i += 1
        else:
            # Group consecutive bit-fields that share the same underlying type.
            group = []
            # Get container info from the first bit-field in the group.
            container_type = desc['type']
            container_size, fmt_code = get_type_info(container_type)
            bits_remaining = container_size * 8
            # For each bit-field that fits in the current container, record its bit offset.
            while i < len(field_descriptors) and field_descriptors[i]['bit_field'] and field_descriptors[i]['type'] == container_type:
                bits_needed = field_descriptors[i]['bits']
                if bits_needed > bits_remaining:
                    break  # Start a new group if the current field does not fit.
                group.append({
                    'name': field_descriptors[i]['name'],
                    'bits': bits_needed,
                    # Bit offset from the least-significant bit position
                    'offset': (container_size * 8 - bits_remaining)
                })
                bits_remaining -= bits_needed
                i += 1
            ops.append({
                'op': 'bit_group',
                'container_type': container_type,
                'container_size': container_size,
                'fmt_code': fmt_code,
                'fields': group
            })

    def parser(data: bytes):
        """Parses the given binary data according to the compiled schema ops."""
        result = {}
        offset = 0
        for op in ops:
            if op['op'] == 'field':
                desc = op['desc']
                type_name = desc['type']
                size, fmt_code = get_type_info(type_name)
                if 'array' in desc:
                    count = desc['array']
                    total_size = size * count
                    raw = data[offset: offset+total_size]
                    offset += total_size
                    fmt = "<" + (fmt_code * count)
                    values = struct.unpack(fmt, raw)
                    if type_name == "char":
                        # Interpret as fixed-length UTF-8 string;
                        # trim trailing nulls.
                        s = bytes(values).split(b'\x00', 1)[0].decode('utf-8')
                        result[desc['name']] = s
                    elif type_name == "bool":
                        result[desc['name']] = [bool(v) for v in values]
                    else:
                        result[desc['name']] = list(values)
                else:
                    raw = data[offset: offset+size]
                    offset += size
                    fmt = "<" + fmt_code
                    value = struct.unpack(fmt, raw)[0]
                    if type_name == "bool":
                        value = bool(value)
                    elif type_name == "char":
                        value = value.decode('utf-8')
                    result[desc['name']] = value
            elif op['op'] == 'bit_group':
                container_size = op['container_size']
                raw = data[offset: offset+container_size]
                offset += container_size
                fmt = "<" + op['fmt_code']
                container_val = struct.unpack(fmt, raw)[0]
                for field in op['fields']:
                    bits = field['bits']
                    bit_offset = field['offset']
                    mask = (1 << bits) - 1
                    value = (container_val >> bit_offset) & mask
                    # If the underlying type was bool, convert to boolean.
                    if op['container_type'] == 'bool':
                        value = bool(value)
                    result[field['name']] = value
            else:
                raise ValueError("Unknown op")
        return result

    return parser

def get_type_info(type_name: str):
    """
    Returns a tuple (size_in_bytes, struct_format_code) for a given type name.
    Supports basic types per the spec.
    """
    mapping = {
        'bool':   (1, 'B'),   # stored as 1 byte (0 or 1)
        'char':   (1, 'c'),
        'int8':   (1, 'b'),
        'uint8':  (1, 'B'),
        'int16':  (2, 'h'),
        'uint16': (2, 'H'),
        'int32':  (4, 'i'),
        'uint32': (4, 'I'),
        'int64':  (8, 'q'),
        'uint64': (8, 'Q'),
        'float':  (4, 'f'),
        'float32':(4, 'f'),
        'double': (8, 'd'),
        'float64':(8, 'd'),
    }
    if type_name not in mapping:
        raise ValueError(f"Unsupported type: {type_name}")
    return mapping[type_name]


# Example usage:
if __name__ == "__main__":
    # Example schema:
    schema = """
        bool flag;
        int16 value;
        char name[4];
        int8 bitsField:4;
        int8 otherField:4;
    """
    parser = compile_struct_parser(schema)
    
    # Create binary data:
    # For standard fields: flag (1 byte), value (2 bytes little-endian), name (4 bytes)
    # For bit-fields: two int8 fields packed into one byte (first 4 bits for bitsField, next 4 for otherField)
    flag = (1).to_bytes(1, byteorder="little")
    value = (123).to_bytes(2, byteorder="little", signed=True)
    name = b'abcd'
    # Pack bit-fields into one byte: assume bitsField=0b1010 (10) and otherField=0b0101 (5).
    bit_container = (10) | (5 << 4)  # note: bits are packed starting at LSB (first field occupies LSBs)
    bit_container = bit_container.to_bytes(1, byteorder="little")
    data = flag + value + name + bit_container

    parsed = parser(data)
    print(parsed)
    # Expected output:
    # {
    #   'flag': True,
    #   'value': 123,
    #   'name': 'abcd',
    #   'bitsField': 10,
    #   'otherField': 5
    # }
