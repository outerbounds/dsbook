from metaflow import FlowSpec, step

class HelloWorldFlow(FlowSpec):

    @step
    def start(self):
        """Starting point"""
        print("This is start step")
        self.next(self.hello)
    
    @step
    def hello(self):
        """Just saying hi"""
        print("Hello World!")
        self.next(self.end)

    @step
    def end(self):
        """Finish line"""
        print("This is end step")

if __name__ == '__main__':
    HelloWorldFlow()
