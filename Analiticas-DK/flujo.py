# Procesamiento de los mensajes de kafka proviniente de los appliances, con informacion de flujo para su deposito en PostgreSQL
# Se generalizo de tal manera que, del string que se recibe en la variable 'count (1|3|3)', los index pares son entradas, y los impares son salidas
import psycopg2
import faust
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from psycopg2 import extras
from psycopg2 import sql

thread_pool = ThreadPoolExecutor(max_workers=None)

param_dic = {
    "host": "192.168.0.127",
    "database": "dk_omia",
    "user": "postgres",
    "password": "Video2021$"
}
tableName='ingreso_persona'

timeToUpload = 300.0

app = faust.App(
    "ETL-flujo",
    broker='kafka://127.0.0.1:9092',
    value_serializer='json',
)
main_topic = app.topic("analytics-omia")

with open("camarasInfo_flujo.json") as jsonFile:
    jsonCamInfo = json.load(jsonFile)
    jsonFile.close()

listDisaggregatedRecords = []
listRecordStandby = []
setCameraId = set()

@app.agent(main_topic)
async def streamUnbundler(events):
    global listDisaggregatedRecords, setCameraId

    async for event in events:
        keysEvent = [*event]
        if 'lc_person' in keysEvent:
            dictCamera = {}
            dictCamera['in'], dictCamera['out'], index  = 0, 0, 0
            listCounts = [x for x in event['lc_person']['count'].split('|') if x]
            for index, count in enumerate(listCounts):
                if index % 2 == 0:
                    dictCamera['in'] += int(count)
                else:
                    dictCamera['out'] += int(count)

            dictCamera['camera_id'] = event['camera_id']
            setCameraId.add(dictCamera['camera_id'])
            listDisaggregatedRecords.append(dictCamera)

@app.timer(timeToUpload)
async def loadTopostgreSQL():
    global thread_pool
    app.loop.run_in_executor(thread_pool, insert_data)

def insert_data():
    global param_dic, listRecordStandby, tableName
    listRecordInsert = recordGenerator()

    if listRecordInsert or listRecordStandby:
        queryText = "INSERT INTO {table}(id_cc, fecha, hora, acceso_id, nombre_comercial_acceso, piso, ins, outs) VALUES %s;"
        try:
            conn = connect(param_dic)
            if conn is None:
                raise ValueError('Error when trying to connect to the DB ...')
            cur = conn.cursor()

            sqlQuery = sql.SQL(queryText).format(table=sql.Identifier(tableName))
            extras.execute_values(cur, sqlQuery.as_string(cur), listRecordInsert+listRecordStandby)
            conn.commit()

            print(str(cur.rowcount)+" records inserted ("+str(len(listRecordStandby))+" correspond to the backup)")
            listRecordStandby = []
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            listRecordStandby += listRecordInsert
            print(str(len(listRecordStandby))+" records in standby")
        finally:
            if conn is not None:
                conn.close()
    else:
        print("Whitout data")

def recordGenerator():
    global setCameraId, listDisaggregatedRecords
    listRecordFlujo = []
    listRecordAtributosIn = []
    listRecordAtributosOut = []
    if listDisaggregatedRecords:
        for camId in setCameraId:
            listFiltered = [e for e in listDisaggregatedRecords if camId == e['camera_id']]
            if listFiltered:
                income, outflows = 0, 0
                for record in listFiltered:
                    income += int(record['in'])
                    outflows += int(record['out'])
                listRecordFlujo.append(getInfoCam(camId)+(income, outflows))
    listDisaggregatedRecords.clear()
    setCameraId.clear()
    return listRecordFlujo


def getInfoCam(camId):
    global jsonCamInfo
    now = datetime.now()
    date = now.strftime("%m/%d/%Y")
    time = now.strftime("%H:%M:%S")
    try:
        return(jsonCamInfo['id_cc'], date, time, jsonCamInfo[str(camId)]['acceso_id'], jsonCamInfo[str(camId)]['nombre_comercial_acceso'], jsonCamInfo[str(camId)]['piso'])
    except:
        return("-1", date, time, "---", "---", "---")


def connect(params_dic):
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(end=" ")
    #print("Connection successful PostgreSQL")
    return conn
