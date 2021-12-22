from pyarrow.csv import read_csv
import numpy as np

def load_model_movies_mtx():
    genome_dim = read_csv('genome-tags.csv').num_rows
    genome_table = read_csv('genome-scores.csv')
    movie_ids = genome_table['movieId'].to_numpy()
    scores = genome_table['relevance'].to_numpy()
    model_movies_mtx = {}
    for i in range(0, len(scores), genome_dim):
        model_movies_mtx[movie_ids[i]] = scores[i:i+genome_dim]
    return model_movies_mtx, genome_dim

def load_model_users_mtx():
    ratings = read_csv('ratings.csv')
    good = ratings.filter(ratings['rating'].to_numpy() > 3.5)
    ids, counts = np.unique(good['userId'].to_numpy(),
                            return_counts=True)
    movies = good['movieId'].to_numpy()
    model_users_mtx = {}
    idx = 0
    for i, user_id in enumerate(ids):
        model_users_mtx[user_id] = tuple(movies[idx:idx + counts[i]])
        idx += counts[i]
    return model_users_mtx

def load_movie_names():
    import csv
    names = {}
    with open('movies.csv', newline='') as f:
        reader = iter(csv.reader(f))
        next(reader)
        for movie_id, name, _ in reader:
            names[int(movie_id)] = name
    return names