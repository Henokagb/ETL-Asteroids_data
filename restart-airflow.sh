echo "Stopping webserver and scheduler"
kill $(cat airflow/airflow-webserver.pid)
kill $(cat airflow/airflow-scheduler.pid)

sleep 5

echo "Starting webserver and scheduler"
airflow webserver -D
airflow scheduler -D