#### Procesamiento de los mensajes de kafka provinientes de los appliances, con informacion de aforo para su deposito en ostgresql
import faust
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import numpy as np
import psycopg2
from psycopg2 import extras
from psycopg2 import sql

thread_pool = ThreadPoolExecutor(max_workers=None)

param_dic = {
    "host": "192.168.0.127",
    "database": "dk_omia",
    "user": "postgres",
    "password": "Video2021$"
}
tableName = 'dev_aforo'

app = faust.App(
    "aforo",
    broker="kafka://34.227.94.165:9092",
    value_serializer="json",
)
main_topic = app.topic("analytics-omia")
timeToUpload = 300.0

with open("camarasInfo_aforo.json") as jsonFile:
    jsonCamInfo = json.load(jsonFile)
    jsonFile.close()

setZonesId = set()
listDisaggregatedRecords = []
listRecordStandby = []

@app.agent(main_topic)
async def streamRoiUnbundler(events):
    global setZonesId, listDisaggregatedRecords, jsonCamInfo
    
    async for event in events:
        keysEvent = [*event]
        if "roi_person" in keysEvent:
            listCounts = [x for x in event['roi_person']['count'].split('|') if x]
            for index, count in enumerate(listCounts):
                dictCamera = {}
                dictCamera['count'] = int(count)
                dictCamera['camera_id'] = int(event['roi_person']['camera_id'])
                dictCamera['zona_id'] = jsonCamInfo[dictCamera['camera_id']]['zona'][str(index)]
                setZonesId.add(dictCamera['zona_id'])
                listDisaggregatedRecords.append(dictCamera)

@app.timer(timeToUpload)
async def loadTopostgreSQL():
    global thread_pool
    app.loop.run_in_executor(thread_pool, insertData)


def insertData():
    global setCameraId, listDisaggregatedRecords, tableName
    listRecordInsert = recordGenerator()

    if listRecordInsert:
        queryText = "INSERT INTO {table}(id_cc, fecha, hora, zona_id, zona, visitas) VALUES %s;"
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
    global setZonesId, listDisaggregatedRecords
    listRecordAforo = []
    
    if listDisaggregatedRecords:
        for zoneId in setZonesId:
            listFiltered = [e for e in listDisaggregatedRecords if zoneId == e['zona_id']]
            if listFiltered:
                list_count = [int(e["count"]) for e in list_filtered]
                sum_count = sum(list_count)
                listRecordAforo.append(getInfoCam(camId,zoneId)+(sum_count))

    listDisaggregatedRecords.clear()
    setZonesId.clear()
    return list_record

def getInfoCam(camId,zoneId):
    global jsonCamInfo
    now = datetime.now()
    date = now.strftime("%m/%d/%Y")
    time = now.strftime("%H:%M:%S")
    try:
        return(jsonCamInfo[str(camId)]['id_cc'], date, time, zoneId, jsonCamInfo['zona'][zoneId])
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
