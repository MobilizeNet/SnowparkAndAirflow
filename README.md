# AirflowTests

This repository shows an example of using Airflow with Snowflake.

We first present an example running with Spark and the show an equivalent example running in snowpark.

# Setting up Airflow

Airflow has many setup methods. In this case we will install using PIP.

Please read Airflow documentation for more details.
When you install airflow the recommended approach is that is uses a contraints file. There are several constraint files, depending on airflow and python versions.

In this example we will use AIRFLOW version `2.4.0` and Python `3.8`
> NOTE: Python version is important as Snowpark for Python uses 3.8

Ok. Now for installation, go to the command prompt or terminal if you are using BDS, and then run

```bash
pip install snowflake-connector-python[pandas]
pip install snowflake-snowpark-python
pip install "apache-airflow[async,snowflake]==2.4.0" --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.4.0/constraints-3.8.txt
```

Make sure to setup the `AIRFLOW_HOME`
```bash
export AIRFLOW_HOME=$(pwd)/airflow
```

Now we will use NGROK to make Airflow Available publicly.
> This is just for convenience, but I recommend it as it is very helpful

```bash
# install ngrok thru pypi
pip install pyngrok
# you will need to setup your authentication token. Just browse to NGROK website https://ngrok.com/ 
ngrok config add-authtoken 26ZdIb8lhWZ3NJZywWqBKtqrIkT_3jn5R6UxVKYsRBFjA6gED
```

# Building the Spark and SnowPark Jobs

In general you can open a terminal, for example in BDS and then run:

```bash
bash install_airflow_and_spark.sh
```

That will install all dependencies, and help you get started

For more details about this code go to https:\\mobilize.net\blogs
