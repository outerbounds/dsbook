from metaflow import FlowSpec, Parameter, step, JSONType

class JSONParameterFlow(FlowSpec):

    mapping = Parameter('mapping',
                        help="Specify a mapping",
                        default='{"some": "default"}',
                        type=JSONType)

    @step
    def start(self):
        for key, value in self.mapping.items():
            print('key', key, 'value', value)
        self.next(self.end)

    @step
    def end(self):
        print("done!")

if __name__ == '__main__':
    JSONParameterFlow()

