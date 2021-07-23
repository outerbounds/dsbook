from metaflow import FlowSpec, step, Parameter, resources, conda_base, profile

@conda_base(python='3.8.3', libraries={'scikit-learn': '0.24.1'})
class ManyKmeansFlow(FlowSpec):

    num_docs = Parameter('num-docs', help='Number of documents', default=1000000)
    
    @resources(memory=4000)
    @step
    def start(self):
        import scale_data
        docs = scale_data.load_yelp_reviews(self.num_docs)
        self.mtx, self.cols = scale_data.make_matrix(docs)
        self.k_params = list(range(5, 55, 5))
        self.next(self.train_kmeans, foreach='k_params')

    @resources(cpu=4, memory=4000)
    @step
    def train_kmeans(self):
        from sklearn.cluster import KMeans
        self.k = self.input
        with profile('k-means'):
            kmeans = KMeans(n_clusters=self.k, verbose=1, n_init=1)
            kmeans.fit(self.mtx)
        self.clusters = kmeans.labels_
        self.next(self.analyze)
        
    @step
    def analyze(self):
        from analyze_kmeans import top_words
        self.top = top_words(self.k, self.clusters, self.mtx, self.cols)
        self.next(self.join)
    
    @step
    def join(self, inputs):
        self.top = {inp.k: inp.top for inp in inputs}
        self.next(self.end)
        
    @step
    def end(self):
        pass

if __name__ == '__main__':
    ManyKmeansFlow()
