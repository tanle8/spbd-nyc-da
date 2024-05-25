# NYC Data Analysis using Spark and GCP

## Environment Setup

### Create a conda env

```shell
conda create --name data_analysis_spark
```

### Activate the environment

```shell
conda activate data_analysis_spark
```

#### Install PySpark

```shell
conda install -c conda-forge pyspark
```

#### Install Jupyter Notebook

```shell
conda install -c conda-forge notebook
```

### Install necessary packages

```shell
pip install -r requirements.txt
```

### Install GCP CLI (Optional)

1. Install the Google Cloud SDK:
   - Download and install the Google Cloud SDK from the Google Cloud SDK page. It includes the gcloud CLI.
   - Follow the installation instructions for your specific operating system.

1. Initialize the `gcloud` CLI:

- After installation, open a terminal or command prompt.
- Run the initialization command:

    ```shell
    gcloud init
    ```

- Follow the on-screen instructions to authenticate your Google account and set up the default configuration, including the project and compute zone.

## Google Cloud Dataproc

We need to setting up Google Cloud Dataproc (a managed Spark and Hadoop service) to executing Spark jobs on it.

We can do it using its Web UI or `gcloud` CLI

## The dataset

The dataset can be found through this link: https://www.kaggle.com/c/nyc-taxi-trip-duration/data

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page


## Run all the jobs

Give the script the permission to run

```sh
chmod +x run_workflow.sh
```

then run it:

```sh
./run_workflow.sh
```
