from metaflow import FlowSpec, Parameter, step

class ParameterFlow(FlowSpec):

    animal = Parameter("creature",
                       help="Specify an animal",
                       required=True)

    count = Parameter("count",  
                      help="Number of animals",
                      default=1)

    ratio = Parameter("ratio",
                      help="Ratio between 0.0 and 1.0",
                      type=float)

    @step
    def start(self):
        print(self.animal, "is a string of", len(self.animal), "characters")
        print("Count is an integer: %s+1=%s" % (self.count, self.count + 1))
        print("Ratio is a", type(self.ratio), "whose value is", self.ratio)
        self.next(self.end)

    @step
    def end(self):
        print("done!")

if __name__ == '__main__':
    ParameterFlow()

