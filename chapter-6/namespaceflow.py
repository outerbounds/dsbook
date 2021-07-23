from metaflow import FlowSpec, step, get_namespace

class NamespaceFlow(FlowSpec):

    @step
    def start(self):
        print('my namespace is', get_namespace())
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    NamespaceFlow()
