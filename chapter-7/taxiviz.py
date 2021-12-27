from io import BytesIO

CANVAS = {'plot_width': 1000,
          'plot_height': 1000,
          'x_range': (-74.03, -73.92),
          'y_range': (40.70, 40.78)}

def visualize(lat, lon):
    from pandas import DataFrame
    import datashader as ds
    from datashader import transfer_functions as tf
    from datashader.colors import Greys9
    canvas = ds.Canvas(**CANVAS)
    agg = canvas.points(DataFrame({'x': lon, 'y': lat}), 'x', 'y')
    img = tf.shade(agg, cmap=Greys9, how='log')
    img = tf.set_background(img, 'white')
    buf = BytesIO()
    img.to_pil().save(buf, format='png')
    return buf.getvalue()
