from datalog import StartRecordData, DataLogRecord, DataLogReader
from entry_parser import EntryParser, struct_parser

entry_parser = EntryParser()

if __name__ == "__main__":
    import mmap
    import sys
    from datetime import datetime

    if len(sys.argv) != 2:
        print("Usage: datalog.py <file>", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1], "r") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        reader = DataLogReader(mm)
        if not reader:
            print("not a log file", file=sys.stderr)
            sys.exit(1)

        entries = {}
        for record in reader:
            timestamp = record.timestamp / 1000000
            if record.isStart():
                #try:
                data = record.getStartData()
                print(
                    f"Start({data.entry}, name='{data.name}', type='{data.type}', metadata='{data.metadata}') [{timestamp}]"
                )
                if data.entry in entries:
                    print("...DUPLICATE entry ID, overriding")
                entries[data.entry] = data

                if data.type == "structschema":
                    entry_parser.register_type(data.name.split('/')[-1])

                # except TypeError:
                #     print("Start(INVALID)")
            elif record.isFinish():
                try:
                    entry = record.getFinishEntry()
                    print(f"Finish({entry}) [{timestamp}]")
                    if entry not in entries:
                        print("...ID not found")
                    else:
                        del entries[entry]
                except TypeError:
                    print("Finish(INVALID)")
            elif record.isSetMetadata():
                try:
                    data = record.getSetMetadataData()
                    print(f"SetMetadata({data.entry}, '{data.metadata}') [{timestamp}]")
                    if data.entry not in entries:
                        print("...ID not found")
                except TypeError:
                    print("SetMetadata(INVALID)")
            elif record.isControl():
                print("Unrecognized control record")
            else:
                #print(f"Data({record.entry}, size={len(record.data)}) ", end="")
                entry = entries.get(record.entry)
                if entry is None:
                    print("<ID not found>")
                    continue

                if entry.type == "structschema":
                    entry_parser.define_parser(entry.name.split('/')[-1], struct_parser(record.getString()))
                    print(f"<name='{entry.name}', type='{entry.type}', data = '{record.getString()}'> [{timestamp}]")

                try:
                    #val = entry_parser()
                    pass
                except TypeError:
                    print("  invalid")