from metaflow import FlowSpec, conda_base, step, profile, resources, Parameter
from importlib import import_module

@conda_base(python='3.8.3',
            libraries={'scikit-learn': '0.24.1', 'numba': '0.53.1'})
class CoocFlow(FlowSpec):

    algo = Parameter('algo', help='Co-oc Algorithm', default='plain')
    num_cpu = Parameter('num-cpu', help='Number of CPU cores', default=32)
    num_docs = Parameter('num-docs', help='Number of documents', default=1000)

    @resources(memory=4000)
    @step
    def start(self):
        import scale_data
        docs = scale_data.load_yelp_reviews(self.num_docs)
        self.mtx, self.cols = scale_data.make_matrix(docs, binary=True)
        print("matrix size: %dx%d" % self.mtx.shape)
        self.next(self.compute_cooc)
    
    @resources(cpu=32, memory=64000)
    @step
    def compute_cooc(self):
        module = import_module('cooc_%s' % self.algo)
        with profile('Computing co-occurrences with the %s algorithm' % self.algo):
            self.cooc = module.compute_cooc(self.mtx, self.num_cpu)
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    CoocFlow()
