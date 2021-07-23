from metaflow import FlowSpec, Flow, step, project

@project(name='demo_project')
class SecondFlow(FlowSpec):

    @step
    def start(self):
        self.model = Flow('FirstFlow').latest_run.data.model
        print('model:', self.model)
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    SecondFlow()
