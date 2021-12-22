import numpy as np

def make_user_vectors(movie_sets, model_movies_mtx):
    user_vector = next(iter(model_movies_mtx.values())).copy()
    for user_id, movie_set in movie_sets:
        user_vector.fill(0)
        for movie_id in movie_set:
            if movie_id in model_movies_mtx:
                user_vector += model_movies_mtx[movie_id]
        yield user_id,\
              movie_set,\
              user_vector / np.linalg.norm(user_vector)