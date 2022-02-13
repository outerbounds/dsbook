from metaflow import FlowSpec, step, conda, Parameter,\
                     S3, resources, project, Flow
import taxiviz
URL = 's3://ursa-labs-taxi-data/2014/'
NUM_SHARDS = 4

def process_data(table):
    return table.filter(table['passenger_count'].to_numpy() > 1)

@project(name='taxi_nyc')
class TaxiPlotterFlow(FlowSpec):

    use_ctas = Parameter('use_ctas_data', help='Use CTAS data', default=False)

    @conda(python='3.8.10')
    @step
    def start(self):
        if self.use_ctas:
            self.paths = Flow('TaxiETLFlow').latest_run.data.paths
        else:
            with S3() as s3:
                objs = s3.list_recursive([URL])
                self.paths = [obj.url for obj in objs]
        #self.paths = athena_ctas()
        print("Processing %d Parquet files" % len(self.paths))
        n = round(len(self.paths) / NUM_SHARDS)
        self.shards = [self.paths[i*n:(i+1)*n] for i in range(NUM_SHARDS - 1)]
        self.shards.append(self.paths[(NUM_SHARDS - 1) * n:])
        self.next(self.preprocess_data, foreach='shards')

    @resources(memory=16000)
    @conda(python='3.8.10', libraries={'pyarrow': '5.0.0'})
    @step
    def preprocess_data(self):
        with S3() as s3:
            from pyarrow.parquet import ParquetDataset
            if self.input:
                objs = s3.get_many(self.input)
                orig_table = ParquetDataset([obj.path for obj in objs]).read()
                self.num_rows_before = orig_table.num_rows
                table = process_data(orig_table)
                self.num_rows_after = table.num_rows
                print('selected %d/%d rows'\
                      % (self.num_rows_after, self.num_rows_before))
                self.lat = table['pickup_latitude'].to_numpy()
                self.lon = table['pickup_longitude'].to_numpy()
        self.next(self.join)

    @resources(memory=16000)
    @conda(python='3.8.10', libraries={'pyarrow': '5.0.0', 'datashader': '0.13.0'})
    @step
    def join(self, inputs):
        import numpy
        lat = numpy.concatenate([inp.lat for inp in inputs])
        lon = numpy.concatenate([inp.lon for inp in inputs])
        print("Plotting %d locations" % len(lat))
        self.image = taxiviz.visualize(lat, lon)
        self.next(self.end)
    
    @conda(python='3.8.10')
    @step
    def end(self):
        pass

if __name__ == '__main__':
    TaxiPlotterFlow()
