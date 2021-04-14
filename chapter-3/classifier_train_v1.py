from metaflow import FlowSpec, step

class ClassifierTrainFlow(FlowSpec):

    @step
    def start(self):
        from sklearn import datasets
        from sklearn.model_selection import train_test_split

        X, y = datasets.load_wine(return_X_y=True)
        self.train_data,\
        self.test_data,\
        self.train_labels,\
        self.test_labels = train_test_split(X, y, test_size=0.2, random_state=0)
        print("Data loaded successfully")
        self.next(self.end)

    @step
    def end(self):
        self.model = 'nothingburger'
        print('done')

if __name__ == '__main__':
    ClassifierTrainFlow()
