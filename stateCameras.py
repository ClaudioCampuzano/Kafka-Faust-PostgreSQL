# Procesamiento de mensajes de kafka provinientes de los appliances, para monitorear el estado de las camaras atravez de la existencia de mensajes, para su
# deposito en postgreSQL
import faust
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2 import extras
from psycopg2 import sql
import requests
import datetime 

app = faust.App(
    "statusMallPlaza",
    broker='kafka://-:9092',
    value_serializer='json',
)

param_dic = {
    "host": "-",
    "database": "postgres",
    "user": "-",
    "password": "-",
    "port": "5432"
}

tableName='traza'
topic = app.topic("analytics-omia")
exclusions = ('108','115','123','124','125','133','146','112','162')

thread_pool = ThreadPoolExecutor(max_workers=None)
camerasId = dict()
currentTime = datetime.datetime.strptime('2020-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
alerts = set()

@app.agent(topic) 
async def streamUnbundler(events):
    global currentTime, camerasId
    async for event in events:
        currentTime = datetime.datetime.strptime(event["@timestamp"], '%Y-%m-%d %H:%M:%S')
        keysEvent = [*event]
        if "roi_person" in keysEvent:   
            camerasId[int(event["roi_person"]["camera_id"])] = event["@timestamp"]
      
@app.timer(30.0)
async def loadTopostgreSQL():
    global thread_pool
    app.loop.run_in_executor(thread_pool, insert_data) 

    
@app.timer(60.0)
async def sendMsgToSlack():
    global param_dic, tableName, exclusions, currentTime, alerts
    queryText = "SELECT * FROM {TABLA} WHERE cliente='mallplaza' AND camara_id NOT IN %s;".replace('{TABLA}',tableName)
    try:
        conn = connect(param_dic)
        if conn is None:
            raise ValueError('Error when trying to connect to the DB ...')
        cur = conn.cursor()
        cur.execute(queryText,(exclusions,))
        recordDB = cur.fetchall()        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    for record in recordDB:
        try:
            timestampCam = datetime.datetime.strptime(record[3], '%Y-%m-%d %H:%M:%S')
            t_delta = datetime.timedelta(minutes=2.5)
            if not(currentTime - t_delta <= timestampCam <= currentTime + t_delta):
                alerts.add(record[2])
            elif record[2] in alerts:
                alerts.remove(record[2])
        except:
            if record[2] not in alerts:
                alerts.add(record[2])  
    
    message = ''
    if (len(alerts)) >= 15:
        for alert in alerts:
            message += str(alert)+', '
            
        if message:
            sendSlackMsg('Camaras sin info: '+ message)
                    

def sendSlackMsg(message):    
    payload = '{"text":"%s"}' % message
    response = requests.post('https://hooks.slack.com/services/-/-/-',
                            data=payload)    
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
        camerasId = dict()

def connect(params_dic):
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(end=" ")
    #print("Connection successful PostgreSQL")
    return conn
