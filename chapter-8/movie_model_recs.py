from collections import Counter
from tempfile import NamedTemporaryFile
from movie_model import make_user_vectors

RECS_ACCURACY = 100

def find_common_movies(users, model_users_mtx, top_n, exclude=None):
    stats = Counter()
    for user_id in users:
        stats.update(model_users_mtx[user_id])
    if exclude:
        for movie_id in exclude:
            stats.pop(movie_id, None)
    return [int(movie_id)
            for movie_id, _ in stats.most_common(top_n)]

def recommend(movie_sets,
              model_movies_mtx,
              model_users_mtx,
              model_ann,
              num_recs):
    for _, movie_set, vec in make_user_vectors(movie_sets, model_movies_mtx):
        for k in range(10, 100, 10):
            similar_users =\
                model_ann.get_nns_by_vector(vec,
                                            k,
                                            search_k=RECS_ACCURACY)
            recs = find_common_movies(similar_users,
                                      model_users_mtx,
                                      num_recs,
                                      exclude=movie_set)
            if recs:
                break
        yield movie_set, recs

def load_model(run):
    from annoy import AnnoyIndex
    model_ann = AnnoyIndex(run.data.model_dim)
    with NamedTemporaryFile() as tmp:
        tmp.write(run.data.model_ann)
        model_ann.load(tmp.name)
    return model_ann,\
           run.data.model_users_mtx,\
           run.data.model_movies_mtx