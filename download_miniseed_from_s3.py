import boto3
from obspy import read
import io


# Initialize S3 client
s3 = boto3.client('s3')

# Define bucket and key
bucket_name = 'my-miniseed'
object_key = 'my-miniseed/WCI/22014/1/WCI.IU.2014.1'

# Download object to memory
response = s3.get_object(Bucket=bucket_name, Key=object_key)
data_stream = io.BytesIO(response['Body'].read())

# Parse with ObsPy
st = read(data_stream)

# Print the ObsPy Streams
print(st)