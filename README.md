# Kafka-Faust-PostgreSQL

```
sudo apt install virtualenv librocksdb-dev
virtualenv env --python=python3
source env/bin/activate
pip3 install -U faust psycopg2-binary pandas
faust -A procesador worker -l info
```
aforoToSqlServer.py -> Procesamiento de mensajes, y carga de datos de AFORO hacia SQL Server 

flujoToPostgresSql.py -> Procesamiento de mensajes, y carga de datos de FLUJO hacia postgreSQL

flujoToPostgresSql_and_CSVtoFTP.py -> Procesamiento de mensajes, y carga de datos de FLUJO hacia postgreSQL, ademas de generacion de CSV para su carga en FTP

stateCameras.py -> Analisis de mensajes, para verificar estado de camaras

permanenciaToPostgreSql-> Procesamiento de mensajes, y carga de datos de PERMANENCIA (tiempo en pantalla) hacia postgreSQL
