BATCH_SIZE = 128

def data_loader(tensor_shards, target=None):
    import tensorflow as tf
    _, dim = tensor_shards[0].shape
    is_training = target is not None
    def make_batches():
        if is_training:
            out_tensor = tf.reshape(tf.convert_to_tensor(target),
                                    (len(target), 1))
        while True:
            row = 0
            for shard in tensor_shards:
                idx = 0
                while True:
                    x = tf.sparse.slice(shard, [idx, 0], [BATCH_SIZE, dim])
                    n, _ = x.shape
                    if n > 0:
                        if is_training:
                            yield x, tf.slice(out_tensor, [row, 0], [n, 1])
                        else:
                            yield x
                        row += n
                        idx += n
                    else:
                        break
            if not is_training:
                break

    input_sig = tf.SparseTensorSpec(shape=(None, dim))
    if is_training:
        signature = (input_sig, tf.TensorSpec(shape=(None, 1)))
    else:
        signature = input_sig
    dataset = tf.data.Dataset.from_generator(make_batches,\
                output_signature=signature)
    dataset.prefetch(tf.data.AUTOTUNE)
    return input_sig, dataset
