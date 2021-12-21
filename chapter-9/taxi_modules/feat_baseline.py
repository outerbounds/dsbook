from taxi_modules.numpy_encoder import NumpyArrayFeatureEncoder

class FeatureEncoder(NumpyArrayFeatureEncoder):
    NAME = 'baseline'
    FEATURE_LIBRARIES = {}
    CLEAN_FIELDS = ['trip_distance', 'total_amount']
    
    @classmethod
    def encode(cls, table):
        return {
            'actual_distance': table['trip_distance'].to_numpy(),
            'amount': table['total_amount'].to_numpy()
        }
