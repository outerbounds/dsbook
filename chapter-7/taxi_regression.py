from metaflow import FlowSpec, step, conda, Parameter,\
                     conda_base, S3, resources, project, Flow

URLS = ['s3://ursa-labs-taxi-data/2014/10/',
        's3://ursa-labs-taxi-data/2014/11/']
NUM_SHARDS = 4
FIELDS = ['trip_distance', 'total_amount'] 

@conda_base(python='3.8.10')
@project(name='taxi_nyc')
class TaxiRegressionFlow(FlowSpec):
    sample = Parameter('sample', default=0.1)
    use_ctas = Parameter('use_ctas_data', help='Use CTAS data', default=False)

    @step
    def start(self):
        if self.use_ctas:
            self.paths = Flow('TaxiETLFlow').latest_run.data.paths
        else:
            with S3() as s3:
                objs = s3.list_recursive(URLS)
                self.paths = [obj.url for obj in objs]
        print("Processing %d Parquet files" % len(self.paths))
        n = max(round(len(self.paths) / NUM_SHARDS), 1)
        self.shards = [self.paths[i*n:(i+1)*n] for i in range(NUM_SHARDS - 1)]
        self.shards.append(self.paths[(NUM_SHARDS - 1) * n:])
        self.next(self.preprocess_data, foreach='shards')

    @resources(memory=16000)
    @conda(libraries={'pyarrow': '5.0.0'})
    @step
    def preprocess_data(self):
        from table_utils import filter_outliers, sample 
        self.shard = None
        with S3() as s3:
            from pyarrow.parquet import ParquetDataset
            if self.input:
                objs = s3.get_many(self.input)
                table = ParquetDataset([obj.path for obj in objs]).read()
                table = sample(filter_outliers(table, FIELDS), self.sample)
                self.shard = {field: table[field].to_numpy()
                              for field in FIELDS}
        self.next(self.join)

    @resources(memory=8000)
    @conda(libraries={'numpy': '1.21.1'})
    @step
    def join(self, inputs):
        from numpy import concatenate
        self.features = {}
        for f in FIELDS:
            shards = [inp.shard[f] for inp in inputs if inp.shard]
            self.features[f] = concatenate(shards)
        self.next(self.regress)

    @resources(memory=8000)
    @conda(libraries={'numpy': '1.21.1',
                      'scikit-learn': '0.24.1',
                      'matplotlib': '3.4.3'})
    @step
    def regress(self):
        from taxi_model import fit, visualize
        self.model = fit(self.features)
        self.viz = visualize(self.model, self.features)
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    TaxiRegressionFlow()
