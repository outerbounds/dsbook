from metaflow import FlowSpec, step, project

@project(name='demo_project')
class FirstFlow(FlowSpec):

    @step
    def start(self):
        self.model = 'this is a demo model'
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    FirstFlow()
