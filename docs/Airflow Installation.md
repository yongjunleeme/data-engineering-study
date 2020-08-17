These instructions are about installing Airflow on a Ubuntu server (not using Docker). We will be using Python 3. Python 2 is sunsetting

## Airflow Python Module Installation

#### First upgrade apt and install python3 pip

```
sudo apt-get update
sudo apt-get install -y python3-pip
```

#### Next install Airflow and other Python modules we need

```
sudo pip3 install apache-airflow['postgres'] cryptography psycopg2-binary boto3 botocore
```

## airflow:airflow Account Creation

Create a dedicated group and account for Airflow. airflow account will have /var/lib/airflow as its home directory.

```
sudo groupadd airflow
sudo useradd airflow -g airflow -d /var/lib/airflow -m
```

## (Optional) Local Postgres Installation to store Airflow related info (DAGs, Tasks, Variables, Connections and so on)

We don't need this section for the lab session since you will be given pre-created account but if you need to set up a Postgres DB locally, here is what you need.

By default, Airflow will be launced with a SQLite database which is a single thread. Will change this to use a more performant database such as Postgres later.

#### Install Postgres server

```
sudo apt-get install -y postgresql postgresql-contrib
```

#### Next create a user and a database to be used by Airflow to store its data
```
$ sudo su postgres
$ psql
psql (10.12 (Ubuntu 10.12-0ubuntu0.18.04.1))
Type "help" for help.

postgres=# CREATE USER airflow PASSWORD 'airflow';
CREATE ROLE
postgres=# CREATE DATABASE airflow;
CREATE DATABASE
postgres=# \q
$ exit
```

#### Restart Postgres

```
sudo service postgresql restart
```


## Initial Airflow Initialization

#### First install Airflow with the default configuration and will change some configuration

```
sudo su airflow
$ cd ~/
$ mkdir dags
$ AIRFLOW_HOME=/var/lib/airflow airflow initdb
$ ls /var/lib/airflow
airflow.cfg  airflow.db  dags   logs  unittests.cfg
```

#### Now edit /var/lib/airflow/airflow.cfg to do the following 3 things:

 * change the "executor" to LocalExecutor from SequentialExecutor
 * change the db connection string ("sql_alchemy_conn") to point to the local Postgres installed above
   * Here you need to use the ID, HOST, PASSWORD and DATABASE assigned to you
 * change Loadexample setting to False
 
```
[core]
...
executor = LocalExecutor
...
sql_alchemy_conn = postgresql+psycopg2://ID:PASSWORD@HOST:5432/DATABASE
...
load_examples = False
```

#### Reinitialize Airflow DB

```
AIRFLOW_HOME=/var/lib/airflow airflow initdb
```


## Start Airflow Webserver and Scheduler

To start up airflow scheduler and webserver as background services, follow the instructions here. Do this as <b>ubuntu</b> account (<b>not airflow</b>). If you get "[sudo] password for airflow: " error, you are still using airflow as your account. Exit so that you can use "ubuntu" account.


#### Create two files:

sudo vi /etc/systemd/system/airflow-webserver.service

```
[Unit]
Description=Airflow webserver
After=network.target

[Service]
Environment=AIRFLOW_HOME=/var/lib/airflow
User=airflow
Group=airflow
Type=simple
ExecStart=/usr/local/bin/airflow webserver -p 8080
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```

sudo vi /etc/systemd/system/airflow-scheduler.service

```
[Unit]
Description=Airflow scheduler
After=network.target

[Service]
Environment=AIRFLOW_HOME=/var/lib/airflow
User=airflow
Group=airflow
Type=simple
ExecStart=/usr/local/bin/airflow scheduler
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```

#### Run them as startup scripts. 

```
sudo systemctl daemon-reload
sudo systemctl enable airflow-webserver
sudo systemctl enable airflow-scheduler
```

Start them:

```
sudo systemctl start airflow-webserver
sudo systemctl start airflow-scheduler
```

To check the status of the services, run as follow:

```
sudo systemctl status airflow-webserver
sudo systemctl status airflow-scheduler
```


## Dag Installation from this repo:

The last step is to copy the files under keeyong/data-engineering repo's dags folder to /var/lib/airflow/dags

```
sudo su airflow
cd ~/
git clone https://github.com/keeyong/data-engineering.git
cp -r data-engineering/dags/* dags
```

Visit your Airflow Web UI and we should see the DAGs from the repo. Some will have errors displayed and you need to add some variables and connections according to the slides 23 to 25 and 30 of "Airflow Deep-dive" preso.
