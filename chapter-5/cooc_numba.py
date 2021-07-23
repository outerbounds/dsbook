
def compute_cooc(mtx, num_cpu):
    from numba import jit
    from cooc_plain import simple_cooc, new_cooc
    cooc = new_cooc(mtx)
    fast_cooc = jit(nopython=True)(simple_cooc)
    fast_cooc(mtx.indptr, mtx.indices, cooc)
    return cooc
