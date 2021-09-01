# Kafka-Faust-PostgreSQL

```
sudo apt install virtualenv librocksdb-dev
virtualenv env --python=python3
source env/bin/activate
pip3 install -U faust psycopg2-binary pandas
faust -A procesador worker -l info
```
procesador.py -> desagregacion de texto, carga a base de datos

procesadorCsv.py -> desagregacion de texto, carga a base de datos y generacion de csv en ftp

statecam.py -> estado de las camaras 

procesadorAlertas.py -> desagregacion de texto, envio desagregacion a nuevo topico
