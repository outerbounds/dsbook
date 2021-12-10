from .dnn_data import data_loader, BATCH_SIZE
from .keras_model import KerasModel
EPOCHS = 4

class Model(KerasModel):
    NAME = 'grid_dnn'
    MODEL_LIBRARIES = {'tensorflow-gpu': '2.6.2'}
    FEATURES = ['grid']

    @classmethod
    def fit(cls, train_data):
        import tensorflow as tf
        input_sig, data = data_loader(train_data['grid']['tensor'],
                                      train_data['baseline']['amount'])
        model = tf.keras.Sequential([
            tf.keras.Input(type_spec=input_sig),
            tf.keras.layers.Dense(2048, activation='relu'),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dense(64, activation='relu'),
            tf.keras.layers.Dense(1)
        ])
        model.compile(loss='mean_squared_error',
                      steps_per_execution=10000,
                      optimizer=tf.keras.optimizers.Adam(0.001))

        num_steps = len(train_data['baseline']['amount']) // BATCH_SIZE
        monitor = tf.keras.callbacks.TensorBoard(update_freq=100)
        model.fit(data,
                  epochs=EPOCHS,
                  verbose=1,
                  steps_per_epoch=num_steps,
                  callbacks=[monitor])
        return model

    @classmethod
    def mse(cls, model, test_data):
        import numpy
        _, data = data_loader(test_data['grid']['tensor'])
        pred = model.predict(data)
        arr = numpy.array([x[0] for x in pred])
        return ((arr - test_data['baseline']['amount'])**2).mean()
