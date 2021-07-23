
def compute_cooc(mtx, num_cpu):
    return (mtx.T * mtx).todense()
