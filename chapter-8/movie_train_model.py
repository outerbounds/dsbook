from metaflow import FlowSpec, step, conda_base, profile, resources
from tempfile import NamedTemporaryFile

ANN_ACCURACY = 100

@conda_base(python='3.8.10', libraries={'pyarrow': '5.0.0',
                                        'python-annoy': '1.17.0'})
class MovieTrainModelFlow(FlowSpec):

    @resources(memory=10000)
    @step
    def start(self):
        import movie_model
        self.model_movies_mtx, self.model_dim =\
            movie_model.load_model_movies_mtx()
        self.model_users_mtx = movie_model.load_model_users_mtx()
        self.movie_names = movie_model.load_movie_names()
        self.next(self.build_annoy_index)

    @resources(memory=10000)
    @step
    def build_annoy_index(self):
        from annoy import AnnoyIndex
        import movie_model
        vectors = movie_model.make_user_vectors(\
                    self.model_users_mtx.items(),
                    self.model_movies_mtx)
        with NamedTemporaryFile() as tmp:
            ann = AnnoyIndex(self.model_dim, 'angular')
            ann.on_disk_build(tmp.name)
            with profile('Add vectors'):
                for user_id, _, user_vector in vectors:
                    ann.add_item(user_id, user_vector)
            with profile('Build index'):
                ann.build(ANN_ACCURACY)
            self.model_ann = tmp.read()
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    MovieTrainModelFlow()