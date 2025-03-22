import pandas as pd
import os

from typing import List, Union
from typing_extensions import override

from format.formatter import Formatter
from format.models import WideRow, LongRow, OutputFormat



class FormatParquet(Formatter):
    def __init__(self, wpilog_file: str,
                 output_file: str,
                 output_format: OutputFormat = OutputFormat.WIDE):
        super().__init__(wpilog_file, output_file, output_format)


    @override
    def convert(self,
                rows: List[Union[WideRow, LongRow]]):
        if not rows:
            raise ValueError(f"No valid records to write to Parquet for {self.output_file}")

        df: pd.DataFrame = pd.DataFrame([row.model_dump() for row in rows])
        # Add filename without extension as the datakey
        filename = os.path.basename(self.wpilog_file)
        df['datakey'] = os.path.splitext(filename)[0]
        print(f"Total Columns being written to parquet file: {len(df.columns)}")
        df.to_parquet(self.output_file)
