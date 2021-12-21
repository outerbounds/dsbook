from itertools import chain
from taxi_modules.table_utils import filter_outliers, sample
from taxi_modules import FEATURES

def execute(table, sample_rate):
    clean_fields = set(chain(*[feat.CLEAN_FIELDS
                               for feat in FEATURES.values()]))
    clean_table = sample(filter_outliers(table, clean_fields), sample_rate)
    print("%d/%d rows included" % (clean_table.num_rows, table.num_rows))
    shards = {}
    for name, encoder in FEATURES.items():
        print("Processing features: %s" % name)
        shards[name] = encoder.encode(clean_table)
    return shards

def merge(train_inputs, test_inputs):
    train_data = {}
    test_data = {}
    for name, encoder in FEATURES.items():
        train_shards = [inp.shards[name] for inp in train_inputs]
        test_shards = [inp.shards[name] for inp in test_inputs]
        train_data[name] = encoder.merge(train_shards)
        test_data[name] = encoder.merge(test_shards)
    return train_data, test_data

