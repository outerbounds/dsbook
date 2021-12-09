import tempfile
EPOCHS = 10

def data_loader(tensor_shards, target=None, batch_size=64):
    import tensorflow as tf
    _, dim = tensor_shards[0].shape

    def make_batches():
        if target is not None:
            out_tensor = tf.reshape(tf.convert_to_tensor(target),
                                    (len(target), 1))
        row = 0
        for shard in tensor_shards:
            idx = 0
            while True:
                x = tf.sparse.slice(shard, [idx, 0], [batch_size, dim])
                n, _ = x.shape
                if n > 0:
                    if target is not None:
                        yield x, tf.slice(out_tensor, [row, 0], [n, 1])
                    else:
                        yield x
                    row += n
                    idx += n
                else:
                    break

    signature = tf.SparseTensorSpec(shape=(None, dim))
    if target is not None:
        signature = (signature, tf.TensorSpec(shape=(None, 1)))

    return tf.data.Dataset.from_generator(make_batches,\
            output_signature=signature)

class Model():
    NAME = 'grid_dnn'
    MODEL_LIBRARIES = {'tensorflow-base': '2.6.0'}
    FEATURES = ['grid']

    @classmethod
    def fit(cls, train_data):
        import tensorflow as tf
        model = tf.keras.Sequential([
            tf.keras.Input(tensor=train_data['grid']['tensor'][0]),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dense(64, activation='relu'),
            tf.keras.layers.Dense(1)
        ])
        model.compile(loss='mean_squared_error',
                      optimizer=tf.keras.optimizers.Adam(0.001))

        tensorboard_callback = tf.keras.callbacks.TensorBoard(update_freq=1000)
        data = data_loader(train_data['grid']['tensor'],
                           train_data['baseline']['amount'])
        model.fit(data,
                  epochs=EPOCHS,
                  verbose=2,
                  callbacks=[tensorboard_callback])
        return model

    @classmethod
    def mse(cls, model, test_data):
        import numpy
        pred = model.predict(data_loader(test_data['grid']['tensor']))
        arr = numpy.array([x[0] for x in pred])
        return ((arr - test_data['baseline']['amount'])**2).mean()

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
