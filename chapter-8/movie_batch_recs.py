from metaflow import FlowSpec, step, conda_base, Parameter,\
                     current, resources, Flow, Run
from collections import Counter
from itertools import chain, combinations, groupby

BATCH_SIZE = 100000
def top_movies(user_movies, top_k):
    stats = Counter(chain.from_iterable(user_movies.values()))
    return [int(k) for k, _ in stats.most_common(top_k)]

@conda_base(python='3.8.10', libraries={'pyarrow': '5.0.0',
                                        'python-annoy': '1.17.0'})
class MovieBatchPredictFlow(FlowSpec):

    num_recs = Parameter('num_recs',
                         help="Number of recommendations per user",
                         default=3)
    num_top_movies = Parameter('num_top',
                               help="Produce recs for num_top movies",
                               default=100)

    @resources(memory=10000)
    @step
    def start(self):
        run = Flow('MovieTrainModelFlow').latest_successful_run
        self.movie_names = run['start'].task['movie_names'].data
        self.model_run = run.pathspec
        print('Using model from', self.model_run)
        model_users_mtx = run['start'].task['model_users_mtx'].data
        self.top_movies = top_movies(model_users_mtx,
                                     self.num_top_movies)
        self.batched_pairs = []
        pairs = enumerate(combinations(self.top_movies, 2))
        for _, batch in groupby(pairs, lambda x: x[0] // BATCH_SIZE):
            self.batched_pairs.append(list(batch))
        self.next(self.batch_score, foreach='batched_pairs')

    @resources(memory=10000)
    @step
    def batch_score(self):
        from movie_model_recs import load_model, recommend
        run = Run(self.model_run)
        model_ann, model_users_mtx, model_movies_mtx = load_model(run)
        self.recs = list(recommend(self.input,
                                   model_movies_mtx,
                                   model_users_mtx,
                                   model_ann,
                                   self.num_recs))
        self.next(self.join)

    @step
    def join(self, inputs):
        import movie_db
        self.model_run = inputs[0].model_run
        names = inputs[0].movie_names
        top = inputs[0].top_movies
        recs = chain.from_iterable(inp.recs for inp in inputs)
        name_data = [(movie_id, int(movie_id in top), name)
                     for movie_id, name in names.items()]
        self.db_version = movie_db.save(current.run_id, recs, name_data)
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    MovieBatchPredictFlow()

