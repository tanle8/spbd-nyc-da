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

- Follow the on-screen instructions to authenticate your Google account and set up your default configuration, including the project and compute zone.

