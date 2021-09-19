class NumpyArrayFeatureEncoder():
    @classmethod
    def encode(cls, table):
        return {}

    @classmethod
    def merge(cls, shards):
        from numpy import concatenate
        return {key: concatenate([shard[key] for shard in shards])
                for key in shards[0]}
