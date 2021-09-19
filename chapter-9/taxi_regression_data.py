from itertools import chain
from metaflow import FlowSpec, step, conda, S3, conda_base,\
                     resources, Flow, project, Parameter
from taxi_modules import init, FEATURES, FEATURE_LIBRARIES
from taxi_modules.table_utils import filter_outliers, sample
init()
TRAIN = ['s3://ursa-labs-taxi-data/2014/09/',
         's3://ursa-labs-taxi-data/2014/10/']
TEST = ['s3://ursa-labs-taxi-data/2014/11/']

@project(name='taxi_regression')
@conda_base(python='3.8.10', libraries={'pyarrow': '3.0.0'})
class TaxiRegressionDataFlow(FlowSpec):
    sample = Parameter('sample', default=0.1)

    @step
    def start(self):
        self.features = FEATURES
        print("Encoding features: %s" % ', '.join(FEATURES))
        with S3() as s3:
            self.shards = []
            for prefix in TEST + TRAIN:
                objs = s3.list_recursive([prefix])
                self.shards.append([obj.url for obj in objs])
        self.next(self.process_features, foreach='shards')

    @resources(memory=16000)
    @conda(libraries=FEATURE_LIBRARIES)
    @step
    def process_features(self):
        from pyarrow.parquet import ParquetDataset
        with S3() as s3:
            objs = s3.get_many(self.input)
            table = ParquetDataset([obj.path for obj in objs]).read()
   
        clean_fields = list(chain(*[FEATURES[feat].CLEAN_FIELDS
                                    for feat in self.features]))
        clean_table = sample(filter_outliers(table, clean_fields),
                             self.sample)
        print("%d/%d rows included" % (clean_table.num_rows, table.num_rows))
        self.shards = {}
        for feat in self.features:
            print("Processing features: %s" % feat)
            self.shards[feat] = FEATURES[feat].encode(clean_table)
        self.next(self.join_data)

    @resources(memory=16000)
    @conda(libraries=FEATURE_LIBRARIES)
    @step
    def join_data(self, inputs):
        #FIXME remove when 2.6 is available in Conda
        from taxi_modules import feat_gridtensor
        feat_gridtensor.install_tf()
        self.train_data = {}
        self.test_data = {}
        self.features = inputs[0].features
        for feat in self.features:
            train_shards = [inp.shards[feat] for inp in inputs[1:]]
            test_shards = [inputs[0].shards[feat]]
            self.train_data[feat] = FEATURES[feat].merge(train_shards)
            self.test_data[feat] = FEATURES[feat].merge(test_shards)
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    TaxiRegressionDataFlow()
