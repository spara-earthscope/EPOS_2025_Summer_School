# Example for Copying and Downloading Data from S3

`miniseed_to_s3.py` copies data from EarthScopes SAGE archive to an AWS s3 bucket that mimics the directory structure and file naming conventions of the archive. Note that the 'miniseed' bucket name is already taken, 'my-miniseed' is used in this example.

`download_miniseed.py` demonstrates how to download miniseed data from S3.

Create the conda environment:

```
conda env create -f environment.yml
```



