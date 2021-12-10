def deep_regression_model(input_sig):
    import tensorflow as tf
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
    return model