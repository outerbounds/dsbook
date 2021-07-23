from concurrent import futures
import math

def compute_cooc_multicore(row_indices, columns, cooc, num_cpu, fast_cooc):
    num_rows = len(row_indices) - 1
    batch_size = math.ceil(num_rows / num_cpu)
    batches = [(cooc.copy(),
                row_indices[i * batch_size:(i+1) * batch_size + 1])
               for i in range(num_cpu)]

    with futures.ThreadPoolExecutor(max_workers=num_cpu) as exe:
        threads = [exe.submit(fast_cooc, row_batch, columns, tmp_cooc)
                   for tmp_cooc, row_batch in batches]
        futures.wait(threads)

    for tmp_cooc, row_batch in batches:
        cooc += tmp_cooc
        
def compute_cooc(mtx, num_cpu):
    from numba import jit
    from cooc_plain import simple_cooc, new_cooc
    cooc = new_cooc(mtx)
    fast_cooc = jit(nopython=True, nogil=True)(simple_cooc)
    fast_cooc(mtx.indptr, mtx.indices, cooc)
    multicore_cooc(mtx.indptr, mtx.indices, cooc, num_cpu, fast_cooc)
    return cooc
