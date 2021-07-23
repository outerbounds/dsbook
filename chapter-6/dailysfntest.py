from metaflow import FlowSpec, Parameter, step, schedule

@schedule(daily=True)
class DailySFNTestFlow(FlowSpec):

    num = Parameter('num',
                    help="Give a number",
                    default=1)

    @step
    def start(self):
        print("The number defined as a parameter is", self.num)
        self.next(self.end)

    @step
    def end(self):
        print('done!')

if __name__ == '__main__':
    DailySFNTestFlow()
