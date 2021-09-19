# Database access PostgresSQL
This package provides different functionialities and structures to store timeseries into a sqlite or postgres database.

## Install
```Terminal
sudo apt install postgresql
```

## Create and configure tables
Replace usernames and passwords to your needs.

```Terminal
sudo -u postgres psql
```



``` SQL
CREATE USER "grafanawriteuser";  
ALTER USER "grafanawriteuser" WITH PASSWORD 'GrafanaWritePassw0rd';  
CREATE DATABASE plottydb;  
GRANT ALL PRIVILEGES ON DATABASE "plottydb" TO "grafanawriteuser";  
GRANT CONNECT ON DATABASE plottydb TO "grafanawriteuser";  
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "grafanawriteuser";  

CREATE USER "grafanaread"; 
ALTER USER "grafanaread" WITH PASSWORD 'GrafanaReadPassw0rd';  
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "grafanaread";  
GRANT CONNECT ON DATABASE plottydb TO "grafanaread";  
GRANT USAGE ON SCHEMA public TO "grafanaread";  
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO "grafanaread";  
GRANT SELECT ON ALL TABLES IN SCHEMA public TO "grafanaread";  

ALTER DATABASE plottydb SET statement_timeout = '60s';

```

### Adapt config to be able to connect from different hosts

```Terminal
sudo vim /etc/postgresql/11/main/pg_hba.conf
```


```vim
## IPv4 local connections
#host    all             all             127.0.0.1/32            md5 #comment this  
host  all  all 0.0.0.0/0 md5 ## add this  

```

```Terminal
sudo vim /etc/postgresql/11/main/postgresql.conf
```

```vim
listen_addresses = '*'
```

### restart service and check

```Terminal
sudo systemctl status postgresql.service
ps -f -u  postgres
sudo lsof -n -u postgres | grep LISTEN
sudo netstat -ltnp | grep postgres
```

### backup db

```Terminal
pg_dump -h 127.0.0.1 --user webuser -d plottydb --format plain --file "2019_09_10-livingroom.dump" -t climate
```

### restore db

```Terminal
sudo su postgres 
postgres@raspberrypi:/home/pi/data$ psql  plottydb < 2019_10_11-livingroom.pgsql
```

### Correct timeshift

``` SQL
UPDATE wetter_data SET timestamp = timestamp - INTERVAL '2 hour' WHERE timestamp > '2020-10-24 20:00:00.000';
UPDATE weatherts SET humidity = 100.0 WHERE humidity > 4000;
```

### Others useful commands

``` SQL
postgres-# DROP OWNED BY webuser;
postgres-# DROP USER "webuser";
sudo su postgres
psql -U postgres -d plottydb -c "Select count(timestamp) from climate;"
psql -U postgres -d plottydb -h 127.0.0.1 -c "Select count(timestamp) from climate;"
select * from climate order by timestamp desc limit 20;
select * from climate where timestamp > '2018-11-04 14:45:28.367' order by timestamp asc limit 10;
```

## Timeseries

### Needs postgres 11 or 12

[TimescaleDB Docs install](https://docs.timescale.com/latest/getting-started/installation/ubuntu/installation-apt-ubuntu)
To Upgrade:
    - Dump database https://www.postgresql.org/docs/12/upgrading.html
    ```terminal
        pg_dumpall > outputfile
    
    ```
    - Change port of old postgres to something different (e.g. 5433)(postgresql.conf)
    - Install new postgres and change port to 5432
    - Configure database as old one
    - Deactivate old one [stackoverflow]("https://serverfault.com/questions/542385/how-to-disable-1-version-of-postgresql-server-without-uninstalling-it/542390")
    - disable old service -> vim /etc/postgresql/10/main/start.conf
    - restart: sudo service postgresql restart

Connect to database (sudo -u postgres psql)

``` SQL
CREATE EXTENSION IF NOT EXISTS timesc;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
```

// create timeseries db

``` SQL
CREATE TABLE measurements (
 time       TIMESTAMP(3)        NOT NULL,
 tag        TEXT                NOT NULL,
 value      DOUBLE PRECISION    NULL,
 comment    TEXT                DEFAULT '',
 unique (time, tag)
);


SELECT * FROM create_hypertable('measurements','time');
```

timescaledb-parallel-copy --verbose  --connection="host=localhost user=webuser password=PlottyPW" --db-name ts_database --table forcex --file old_forcex.csv --workers 4 --copy-options "CSV"

- [TimescaleDB Reading](https://docs.timescale.com/latest/using-timescaledb/reading-data)
- [TimescaleDB hypertables](https://docs.timescale.com/latest/using-timescaledb/hypertables)
- [TimescaleDB migrating data](https://docs.timescale.com/latest/getting-started/migrating-data)

```psql
CREATE TABLE new_table (LIKE old_table INCLUDING DEFAULTS INCLUDING 
CONSTRAINTS INCLUDING INDEXES);
```
