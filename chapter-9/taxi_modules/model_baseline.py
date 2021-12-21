from taxi_modules.regression import RegressionModel

class Model(RegressionModel):
    NAME = 'distance_regression'
    MODEL_LIBRARIES = {'scikit-learn': '0.24.1'}
    FEATURES = ['baseline']
    regressor = 'actual_distance'
