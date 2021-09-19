
class RegressionModel():

    @classmethod
    def match(cls, features):
        return cls.FEATURE in features

    @classmethod
    def mse(cls, model, test_data):
        from sklearn.metrics import mean_squared_error
        d = test_data[cls.FEATURE][cls.FIELD].reshape(-1, 1)
        pred = model.predict(d)
        return mean_squared_error(test_data['baseline']['amount'], pred)

    @classmethod
    def fit(cls, train_data):
        from sklearn.linear_model import LinearRegression
        d = train_data[cls.FEATURE][cls.FIELD].reshape(-1, 1)
        model = LinearRegression().fit(d, train_data['baseline']['amount'])
        return model

    @classmethod
    def visualize(cls, model, test_data):
        import matplotlib.pyplot as plt
        from io import BytesIO
        import numpy
        column = test_data[cls.FEATURE][cls.FIELD]
        maxval = max(column)
        line = numpy.arange(0, maxval, maxval / 1000)
        pred = model.predict(line.reshape(-1, 1))
        data={cls.FIELD: column, 'amount': test_data['baseline']['amount']}
        plt.rcParams.update({'font.size': 22})
        plt.scatter(data=data, x=cls.FIELD,
                    y='amount', alpha=0.01, linewidth=0.5)
        plt.plot(line, pred, linewidth=2, color='black')
        fig = plt.gcf()
        fig.set_size_inches(18.5, 10.5)
        fig.suptitle(cls.NAME)
        buf = BytesIO()
        fig.savefig(buf)
        return buf.getvalue()

    @classmethod
    def save_model(cls, model):
        return model

    @classmethod
    def load_model(cls, model):
        return model

