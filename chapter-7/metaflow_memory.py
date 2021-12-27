from functools import wraps

def profile_memory(f):
    @wraps(f)
    def func(self):
        from memory_profiler import memory_usage
        self.mem_usage = memory_usage((f, (self,), {}),
                                      max_iterations=1,
                                      max_usage=True,
                                      interval=0.2)
    return func

