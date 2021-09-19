from taxi_modules.regression import RegressionModel

class Model(RegressionModel):
    NAME = 'euclidean_regression'
    MODEL_LIBRARIES = {'scikit-learn': '0.24.1', 'matplotlib': '3.4.3'}
    FEATURE = 'euclidean'
    FIELD = 'euclidean_distance'
