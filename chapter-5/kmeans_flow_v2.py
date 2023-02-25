from metaflow import FlowSpec, step, Parameter, resources, conda_base, profile

@conda_base(python='3.8.3', libraries={'scikit-learn': '0.24.1', 'boto3':'1.24.77'})
class KmeansFlow(FlowSpec):

    num_docs = Parameter('num-docs', help='Number of documents', default=1000000)
    
    @resources(memory=4000)
    @step
    def start(self):
        import scale_data
        docs = scale_data.load_yelp_reviews(self.num_docs)
        self.mtx, self.cols = scale_data.make_matrix(docs)
        print("matrix size: %dx%d" % self.mtx.shape)
        self.next(self.train_kmeans)

    @resources(cpu=16, memory=4000)
    @step
    def train_kmeans(self):
        from sklearn.cluster import KMeans
        with profile('k-means'):
            kmeans = KMeans(n_clusters=10, verbose=1, n_init=1)
            kmeans.fit(self.mtx)
        self.clusters = kmeans.labels_
        self.next(self.end)
        
    @step
    def end(self):
        pass

if __name__ == '__main__':
    KmeansFlow()
