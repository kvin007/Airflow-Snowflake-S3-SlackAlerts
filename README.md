# Airflow-Snowflake-S3 with Slack Alerts
This shows how to use Airflow with Snowflake and S3. Also sending Slack Alerts in case a task fails

I first upload the data from this Kaggle dataset: https://www.kaggle.com/megelon/meetup to Snowflake.
If you don't have an account, you can create a free one.

I used [queries performed.sql] to load the data into Snowflake. You can create a Worksheet in Snowflake or use SnowSQL


I took the official Airflow documentation (https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html) as a reference and made some modifications (added a Dockerfile with some requirements.txt)
In order to run the project, you will need to install Docker Desktop (if not already installed) and execute the following:
docker-compose build
docker-compose up

After you access airflow locally, you need to create an S3 bucket in AWS and the Connections in Airflow (for snowflake, S3 and Slack)
For the Slack notifications, you will need to create a Slack workspace and an App. I followed this Medium Post by Kaxil Naik (https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105)

After everything is setup, you can execute the DAG. What is basically does is runs everday and inserts new data into a destination table.



