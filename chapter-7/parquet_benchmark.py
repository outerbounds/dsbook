import os
from metaflow import FlowSpec, step, conda_base, resources, S3, profile

URL = 's3://ursa-labs-taxi-data/2014/12/data.parquet'

@conda_base(python='3.8.10',
        libraries={'pyarrow': '5.0.0', 'pandas': '1.3.2', 'memory_profiler': '0.58.0'})
class ParquetBenchmarkFlow(FlowSpec):

    @step
    def start(self):
        import pyarrow.parquet as pq
        with S3() as s3:
            res = s3.get(URL)
            table = pq.read_table(res.path)
            os.rename(res.path, 'taxi.parquet')
        table.to_pandas().to_csv('taxi.csv')
        self.stats = {}
        self.next(self.load_csv, self.load_parquet, self.load_pandas)

    @step
    def load_csv(self):
        with profile('load_csv', stats_dict=self.stats):
            import csv
            with open('taxi.csv') as csvfile:
                for row in csv.reader(csvfile):
                    pass
        self.next(self.join)

    @step
    def load_parquet(self):
        with profile('load_parquet', stats_dict=self.stats):
            import pyarrow.parquet as pq
            table = pq.read_table('taxi.parquet')
        self.next(self.join)
    
    @step
    def load_pandas(self):
        with profile('load_pandas', stats_dict=self.stats):
            import pandas as pd
            df = pd.read_parquet('taxi.parquet')
        self.next(self.join)

    @step
    def join(self, inputs):
        for inp in inputs:
            print(list(inp.stats.items())[0])
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    ParquetBenchmarkFlow()

