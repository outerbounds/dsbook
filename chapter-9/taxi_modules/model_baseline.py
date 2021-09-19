from taxi_modules.regression import RegressionModel

class Model(RegressionModel):
    NAME = 'distance_regression'
    MODEL_LIBRARIES = {'scikit-learn': '0.24.1', 'matplotlib': '3.4.3'}
    FEATURE = 'baseline'
    FIELD = 'actual_distance'
