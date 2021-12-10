from .dnn_data import data_loader, BATCH_SIZE
from .keras_model import KerasModel
from .dnn_model import deep_regression_model
EPOCHS = 4

class Model(KerasModel):
    NAME = 'grid_dnn'
    MODEL_LIBRARIES = {'tensorflow-base': '2.6.0'}
    FEATURES = ['grid']

    @classmethod
    def fit(cls, train_data):
        import tensorflow as tf
        input_sig, data = data_loader(train_data['grid']['tensor'],
                                      train_data['baseline']['amount'])
        model = deep_regression_model(input_sig)
        monitor = tf.keras.callbacks.TensorBoard(update_freq=100)
        num_steps = len(train_data['baseline']['amount']) // BATCH_SIZE
        model.fit(data,
                  epochs=EPOCHS,
                  verbose=2,
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
