from metaflow import Flow
from flask import Flask, request

from movie_model import load_model, recommend
from movie_data import load_movie_names

class RecsModel():
    def __init__(self):
        self.run = Flow('MovieTrainFlow').latest_successful_run
        self.model_ann,\
        self.model_users_mtx,\
        self.model_movies_mtx = load_model(self.run)
        self.names = load_movie_names()

    def get_recs(self, movie_ids, num_recs):
        [(_, recs)] = list(recommend([(None, set(movie_ids))],
                                     self.model_movies_mtx,
                                     self.model_users_mtx,
                                     self.model_ann,
                                     num_recs))
        return recs

    def get_names(self, ids):
        return '\n'.join(self.names[movie_id] for movie_id in ids)

    def version(self):
        return self.run.pathspec

print("Loading model")
model = RecsModel()
print("Model loaded")
app = Flask(__name__)

def preprocess(ids_str, model, response):
    ids = list(map(int, ids_str.split(',')))
    response.write("# Model version:\n%s\n" % model.version())
    response.write("# Input movies\n%s\n" % model.get_names(ids))
    return ids

def postprocess(recs, model, response):
    response.write("# Recommendations\n%s\n" % model.get_names(recs))

@app.route("/recommend")
def recommend_endpoint():
    response = StringIO()
    ids = preprocess(request.args.get('ids'), model, response)
    num_recs = int(request.args.get('num', 3))
    recs = model.get_recs(ids, num_recs)
    postprocess(recs, model, response)
    return response.getvalue()
