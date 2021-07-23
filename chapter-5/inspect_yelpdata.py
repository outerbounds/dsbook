import tarfile
from metaflow import S3

with S3() as s3:
    res = s3.get('s3://fast-ai-nlp/yelp_review_full_csv.tgz')
    with tarfile.open(res.path) as tar:
        datafile = tar.extractfile('yelp_review_full_csv/train.csv')
        reviews = [line.decode('utf-8') for line in datafile]

print('\n'.join(reviews[:2]))  

