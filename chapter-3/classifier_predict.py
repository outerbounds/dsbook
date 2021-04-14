from metaflow import FlowSpec, step, Flow, Parameter, JSONType

class ClassifierPredictFlow(FlowSpec):

    vector = Parameter('vector', type=JSONType, required=True)

    @step
    def start(self):
        run = Flow('ClassifierTrainFlow').latest_run
        self.train_run_id = run.pathspec
        self.model = run['end'].task.data.model
        print("Input vector", self.vector)
        self.next(self.end)

    @step
    def end(self):
        print("Predicted class", self.model.predict([self.vector])[0])

if __name__ == '__main__':
    ClassifierPredictFlow()
