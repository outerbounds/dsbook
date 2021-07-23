from metaflow import FlowSpec, step, retry, catch

class CatchDivideByZeroFlow(FlowSpec):

    @step
    def start(self):
        self.divisors = [0, 1, 2]
        self.next(self.divide, foreach='divisors')

    @catch(var='divide_failed')
    @retry(times=2)
    @step
    def divide(self):
        self.res = 10 / self.input
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = [inp.res for inp in inputs if not inp.divide_failed]
        print('results', self.results)
        self.next(self.end)

    @step
    def end(self):
        print('done!')

if __name__ == '__main__':
    CatchDivideByZeroFlow()

