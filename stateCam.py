import faust
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2 import extras
from psycopg2 import sql

app = faust.App(
    "status",
    broker='kafka://127.0.0.1:9092',
    value_serializer='json',
)

param_dic = {
    "host": "192.168.0.127",
    "database": "omia",
    "user": "postgres",
    "password": "Video2021feo$",
     "port": "5432"
}

tableName='traza'
topic = app.topic("analytics-omia")

thread_pool = ThreadPoolExecutor(max_workers=None)
camerasId = dict()

@app.agent(topic) 
async def streamUnbundler(events):
    async for event in events:
        """res = next((dicc for dicc in cameraStatus if dicc['id'] == event['lc_person']['camera_id']), None)
        if res == None:
            cameraStatus.append({'id': event['lc_person']['camera_id'],'timestamp': event['@timestamp']})
        else:
            res['timestamp'] = event['@timestamp']"""
        camerasId[int(event["lc_person"]["camera_id"])] = event["@timestamp"]
      
@app.timer(60.0)
async def loadTopostgreSQL():
    global thread_pool
    app.loop.run_in_executor(thread_pool, insert_data)
    
def insert_data():
    global param_dic, tableName, camerasId
    queryText = "UPDATE {table} AS t SET timestamp=e.times FROM (VALUES %s) AS e(id, times) WHERE e.id = t.camara_id;"
    camInfo = [(str(k), str(v)) for k, v in camerasId.items()]
    try:
        conn = connect(param_dic)
        if conn is None:
            raise ValueError('Error when trying to connect to the DB ...')
        cur = conn.cursor()

        sqlQuery = sql.SQL(queryText).format(table=sql.Identifier(tableName))
        extras.execute_values(cur, sqlQuery.as_string(cur), camInfo, template=None, page_size=100)
        conn.commit()

        print("Updated: "+str(cur.rowcount)+" records successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def connect(params_dic):
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(end=" ")
    #print("Connection successful PostgreSQL")
    return conn
