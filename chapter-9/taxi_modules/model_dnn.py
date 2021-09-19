import tempfile

class Model():
    NAME = 'grid_dnn'
    MODEL_LIBRARIES = {} # FIXME: add tensorflow

    @classmethod
    def match(cls, features):
        return 'grid' in features

    @classmethod
    def mse(cls, model, test_data):
        import numpy
        pred = model.predict(test_data['grid']['tensor'])
        arr = numpy.array([x[0] for x in pred])
        return ((arr - test_data['baseline']['amount'])**2).mean()

    @classmethod
    def fit(cls, train_data):
        import tensorflow as tf
        model = tf.keras.Sequential([
            tf.keras.Input(shape=(10001,)),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dense(64, activation='relu'),
            tf.keras.layers.Dense(1)
        ])
        model.compile(loss='mean_squared_error',
                      optimizer=tf.keras.optimizers.Adam(0.001))
        model.fit(x=train_data['grid']['tensor'],
                  y=train_data['baseline']['amount'],
                  epochs=2,
                  verbose=2)
        return model

    @classmethod
    def visualize(cls, model, test_data):
        return None

    @classmethod
    def save_model(cls, model):
        import tensorflow as tf
        with tempfile.NamedTemporaryFile() as f:
            tf.keras.models.save_model(model, f.name, save_format='h5')
            return f.read()

    @classmethod
    def load_model(cls, blob):
        import tensorflow as tf
        with tempfile.NamedTemporaryFile() as f:
            f.write(blob)
            f.flush()
            return tf.keras.models.load_model(f.name)
