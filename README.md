# Kafka-Faust-PostgreSQL

```
sudo apt install virtualenv librocksdb-dev
virtualenv env --python=python3
source env/bin/activate
pip3 install -U faust psycopg2-binary pandas
faust -A procesador worker -l info
```
