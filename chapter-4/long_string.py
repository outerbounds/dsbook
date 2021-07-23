from metaflow import batch, FlowSpec, step

LENGTH = 8_000_000_000

class LongStringFlow(FlowSpec):

    @step
    def start(self):
        long_string = b'x' * LENGTH
        print("lots of memory consumed!")
        self.next(self.end)
    
    @step
    def end(self):
        print('done!')

if __name__ == '__main__':
    LongStringFlow()

