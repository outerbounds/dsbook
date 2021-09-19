from metaflow import FlowSpec, step, conda, S3, conda_base,\
                     resources, Flow, project, profile
from taxi_modules import init, MODELS, MODEL_LIBRARIES
init()

@project(name='taxi_regression')
@conda_base(python='3.8.10', libraries={'pyarrow': '5.0.0'})
class TaxiRegressionModelFlow(FlowSpec):

    @step
    def start(self):
        run = Flow('TaxiRegressionDataFlow').latest_run
        self.data_run_id = run.id
        self.features = run.data.features
        self.models = [name for name, model in MODELS.items()
                       if model.match(self.features)]
        print("Building models: %s" % ', '.join(self.models))
        self.next(self.train_model, foreach='models')

    @resources(memory=16000)
    @conda(libraries=MODEL_LIBRARIES)
    @step
    def train_model(self):
        #FIXME remove when 2.6 is available in Conda
        from taxi_modules import feat_gridtensor
        feat_gridtensor.install_tf()
        self.model_name = self.input
        with profile('Training model: %s' % self.model_name):
            mod = MODELS[self.model_name]
            data_run = Flow('TaxiRegressionDataFlow')[self.data_run_id]
            model = mod.fit(data_run.data.train_data)
            self.model = mod.save_model(model)
        self.next(self.predict)

    @resources(memory=16000)
    @conda(libraries=MODEL_LIBRARIES)
    @step
    def predict(self):
        with profile("Predicting using %s" % self.model_name):
            mod = MODELS[self.model_name]
            data_run = Flow('TaxiRegressionDataFlow')[self.data_run_id]
            model = mod.load_model(self.model)
            self.mse = mod.mse(model, data_run.data.test_data)
        self.viz = mod.visualize(model, data_run.data.test_data)
        self.next(self.join)

    @step
    def join(self, inputs):
        for inp in inputs:
            print("MODEL %s MSE %f" % (inp.model_name, inp.mse))
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    TaxiRegressionModelFlow()


