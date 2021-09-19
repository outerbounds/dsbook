import os
from importlib import import_module

MODELS = {}
FEATURES = {}
FEATURE_LIBRARIES = {}
MODEL_LIBRARIES = {}

def init():
    for fname in os.listdir(os.path.dirname(__file__)):
        is_feature = fname.startswith('feat_')
        is_model = fname.startswith('model_')
        if is_feature or is_model:
            mod = import_module('taxi_modules.%s' % fname.split('.')[0])
            if is_feature:
                cls = mod.FeatureEncoder
                FEATURES[cls.NAME] = cls
                FEATURE_LIBRARIES.update(cls.FEATURE_LIBRARIES.items())
            else:
                cls = mod.Model
                MODELS[cls.NAME] = cls
                MODEL_LIBRARIES.update(cls.MODEL_LIBRARIES.items())
