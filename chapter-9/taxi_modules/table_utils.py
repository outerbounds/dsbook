def filter_outliers(table, clean_fields):
    import numpy
    valid = numpy.ones(table.num_rows, dtype='bool')
    for field in clean_fields:
        column = table[field].to_numpy()
        minval = numpy.percentile(column, 2)
        maxval = numpy.percentile(column, 98)
        valid &= (column > minval) & (column < maxval)
    return table.filter(valid)

def sample(table, p):
    import numpy
    return table.filter(numpy.random.random(table.num_rows) < p)



