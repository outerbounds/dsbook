from metaflow import FlowSpec, step

class WideForeachFlow(FlowSpec):

    @step
    def start(self):
        self.ints = list(range(1000))
        self.next(self.multiply, foreach='ints')
    
    @step
    def multiply(self):
        self.result = self.input * 1000
        self.next(self.join)
    
    @step
    def join(self, inputs):
        self.total = sum(inp.result for inp in inputs)
        self.next(self.end)

    @step
    def end(self):
        print('Total sum is', self.total)

if __name__ == '__main__':
    WideForeachFlow()

