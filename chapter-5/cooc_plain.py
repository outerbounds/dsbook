
def simple_cooc(indptr, indices, cooc):
    for row_idx in range(len(indptr) - 1):
        row = indices[indptr[row_idx]:indptr[row_idx+1]]
        row_len = len(row)
        for i in range(row_len):
            x = row[i]
            cooc[x][x] += 1 # update the diagonal
            for j in range(i + 1, row_len):
                y = row[j]
                cooc[x][y] += 1
                cooc[y][x] += 1

def new_cooc(mtx):
    import numpy as np
    num_words = mtx.shape[1]
    return np.zeros((num_words, num_words), dtype=np.int32)

def compute_cooc(mtx, num_cpu):
    cooc = new_cooc(mtx)
    simple_cooc(mtx.indptr, mtx.indices, cooc)
    return cooc
        
