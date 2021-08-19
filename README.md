# Kafka-Faust-PostgreSQL

```
sudo apt install virtualenv librocksdb-dev
virtualenv env --python=python3
source venv/bin/activate
pip3 install -U faust psycopg2-binary
faust -A procesador worker -l info
```
