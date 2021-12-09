from metaflow import profile
NUM_HASH_BINS = 10000
PRECISION = 6

class FeatureEncoder():
    NAME = 'grid'
    FEATURE_LIBRARIES = {'python-geohash': '0.8.5',
                         'tensorflow-base': '2.6.0'}
    CLEAN_FIELDS = ['pickup_latitude', 'pickup_longitude',
                    'dropoff_latitude', 'dropoff_longitude']

    @classmethod
    def _coords_to_grid(cls, table):
        import geohash
        plon = table['pickup_longitude'].to_numpy()
        plat = table['pickup_latitude'].to_numpy()
        dlon = table['dropoff_longitude'].to_numpy()
        dlat = table['dropoff_latitude'].to_numpy()
        trips = []
        for i in range(len(plat)):
            pcode = geohash.encode(plat[i], plon[i], precision=PRECISION)
            dcode = geohash.encode(dlat[i], dlon[i], precision=PRECISION)
            trips.append((pcode, dcode))
        return trips

    @classmethod
    def encode(cls, table):
        from tensorflow.keras.layers import Hashing, IntegerLookup
        with profile('coordinates to grid'):
            grid = cls._coords_to_grid(table)
        hashing_trick = Hashing(NUM_HASH_BINS)
        multi_hot = IntegerLookup(vocabulary=list(range(NUM_HASH_BINS)),
                                  output_mode='multi_hot',
                                  sparse=True)
        with profile('creating tensor'):
            tensor = multi_hot(hashing_trick(grid))
        return {'tensor': tensor}
    
    @classmethod
    def merge(cls, shards):
        return {key: [s[key] for s in shards] for key in shards[0]}