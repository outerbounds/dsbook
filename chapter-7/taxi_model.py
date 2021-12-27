
def fit(features):
    from sklearn.linear_model import LinearRegression
    d = features['trip_distance'].reshape(-1, 1)
    model = LinearRegression().fit(d, features['total_amount'])
    return model

def visualize(model, features):
    import matplotlib.pyplot as plt
    from io import BytesIO
    import numpy
    maxval = max(features['trip_distance'])
    line = numpy.arange(0, maxval, maxval / 1000)
    pred = model.predict(line.reshape(-1, 1))
    plt.rcParams.update({'font.size': 22})
    plt.scatter(data=features,
                x='trip_distance',
                y='total_amount',
                alpha=0.01,
                linewidth=0.5)
    plt.plot(line, pred, linewidth=2, color='black')
    plt.xlabel('Distance')
    plt.ylabel('Amount')
    fig = plt.gcf()
    fig.set_size_inches(18, 10)
    buf = BytesIO()
    fig.savefig(buf)
    return buf.getvalue()
