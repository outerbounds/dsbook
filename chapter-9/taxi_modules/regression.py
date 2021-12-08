class RegressionModel():

    @classmethod
    def fit(cls, train_data):
        from sklearn.linear_model import LinearRegression
        d = train_data[cls.FEATURES[0]][cls.regressor].reshape(-1, 1)
        model = LinearRegression().fit(d, train_data['baseline']['amount'])
        return model

    @classmethod
    def mse(cls, model, test_data):
        from sklearn.metrics import mean_squared_error
        d = test_data[cls.FEATURES[0]][cls.regressor].reshape(-1, 1)
        pred = model.predict(d)
        return mean_squared_error(test_data['baseline']['amount'], pred)

    @classmethod
    def save_model(cls, model):
        return model

    @classmethod
    def load_model(cls, model):
        return model