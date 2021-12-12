BATCH_SIZE = 128

def data_loader(tensor_shards, target=None):
    import tensorflow as tf
    _, dim = tensor_shards[0].shape
    def make_batches():
        if target is not None:
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
                        if target is not None:
                            yield x, tf.slice(out_tensor, [row, 0], [n, 1])
                        else:
                            yield x
                        row += n
                        idx += n
                    else:
                        break
            if target is not None:
                break

    input_sig = tf.SparseTensorSpec(shape=(None, dim))
    if target is None:
        signature = input_sig
    else:
        signature = (input_sig, tf.TensorSpec(shape=(None, 1)))
    dataset = tf.data.Dataset.from_generator(make_batches,\
                output_signature=signature)
    dataset.prefetch(tf.data.AUTOTUNE)
    return input_sig, dataset
