echo "Welcome!"
echo "This script will install snowflake dependencies, airflow and spark"

# Install spark

pip install pyspark

pip install snowflake-connector-python[pandas]
pip install snowflake-snowpark-python
pip install "apache-airflow[async,snowflake]==2.4.0" --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.4.0/constraints-3.8.txt

# setup HOME variables
export AIRFLOW_HOME=$(pwd)/airflow

airflow db init

airflow standalone &

echo "Building EGG jobs"
bash build_snowpark_job.sh
bash build_spark_job.sh

echo "You can find the password at: /home/BlackDiamond/workspace/airflow/standalone_admin_password.txt"


echo "Now we will setup NGROK so it is easier to access your airflow console"
# install ngrok thru pypi
pip install pyngrok
# you will need to setup your authentication token. Just browse to NGROK website https://ngrok.com/ 
echo "Please enter your NGROK token"
read token
ngrok config add-authtoken $token
ngrok http http://0.0.0.0:8080


