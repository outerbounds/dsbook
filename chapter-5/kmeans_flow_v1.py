from metaflow import FlowSpec, step, Parameter, resources

class KmeansFlow(FlowSpec):

    num_docs = Parameter('num-docs', help='Number of documents', default=1000)
    
    @resources(memory=1000)
    @step
    def start(self):
        import scale_data
        scale_data.load_yelp_reviews(self.num_docs)
        self.next(self.end)
    
    @step
    def end(self):
        pass

if __name__ == '__main__':
    KmeansFlow()
