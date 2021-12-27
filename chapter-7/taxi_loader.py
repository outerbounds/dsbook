from metaflow import FlowSpec, Parameter, step, conda, profile, S3

GLUE_DB = 'dsinfra_test'
URL = 's3://ursa-labs-taxi-data/2014/'

TYPES = {'timestamp[us]': 'bigint', 'int8': 'tinyint'}

class TaxiDataLoader(FlowSpec):

    table = Parameter('table',
                      help='Table name',
                      default='nyc_taxi')

    @conda(python='3.8.10', libraries={'pyarrow': '5.0.0'})
    @step
    def start(self):
        import pyarrow.parquet as pq
        
        def make_key(obj):
            key = '%s/month=%s/%s' % tuple([self.table] + obj.key.split('/'))
            return key, obj.path

        def hive_field(f):
            return f.name, TYPES.get(str(f.type), str(f.type)) 

        with S3() as s3down:
            with profile('Dowloading data'):
                loaded = list(map(make_key, s3down.get_recursive([URL])))
            table = pq.read_table(loaded[0][1])
            self.schema = dict(map(hive_field, table.schema))
            with S3(run=self) as s3up:
                with profile('Uploading data'):
                    uploaded = s3up.put_files(loaded)
                key, url = uploaded[0]
                self.s3_prefix = url[:-(len(key) - len(self.table))]
        self.next(self.end)

    @conda(python='3.8.10', libraries={'awswrangler': '1.10.1'})
    @step
    def end(self):
        import awswrangler as wr
        try:
            wr.catalog.create_database(name=GLUE_DB)
        except:
            pass
        wr.athena.create_athena_bucket()
        with profile('Creating table'):
            wr.catalog.create_parquet_table(database=GLUE_DB,
                                            table=self.table,
                                            path=self.s3_prefix,
                                            columns_types=self.schema,
                                            partitions_types={'month': 'int'},
                                            mode='overwrite')
            wr.athena.repair_table(self.table, database=GLUE_DB)

if __name__ == '__main__':
    TaxiDataLoader()
