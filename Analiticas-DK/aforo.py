#### Procesamiento de los mensajes de kafka provinientes de los appliances, con informacion de aforo para su deposito en ostgresql
import faust
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import numpy as np
import psycopg2
from psycopg2 import extras
from psycopg2 import sql
import json
import numpy


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
main_topic = app.topic("roi")
timeToUpload = 30.0

with open("camarasInfo_aforo.json") as jsonFile:
    jsonCamInfo = json.load(jsonFile)
    jsonFile.close()

setZonesId = set()
setCamerasId = set()
listDisaggregatedRecords = []
listRecordStandby = []

@app.agent(main_topic)
async def streamRoiUnbundler(events):
    global setZonesId, listDisaggregatedRecords, jsonCamInfo
    
    async for event in events:
        keysEvent = [*event]
        if "roi_person" in keysEvent:
            print(event)
            listCounts = [x for x in event['roi_person']['count'].split('|') if x]
            for index, count in enumerate(listCounts):
                dictCamera = {}
                dictCamera['count'] = int(count)
                dictCamera['camera_id'] = event['camera_id']

                
                if ((len(listCounts) > 1) and (index != 0)): 
                    dictCamera['zona_id'] = jsonCamInfo[str(event['camera_id'])]['zona'][str(index-1)]
                else:
                    dictCamera['zona_id'] = jsonCamInfo[str(event['camera_id'])]['zona'][str(index)]
                
                setZonesId.add(dictCamera['zona_id'])
                setCamerasId.add(dictCamera['camera_id'])
                listDisaggregatedRecords.append(dictCamera)

@app.timer(timeToUpload)
async def loadTopostgreSQL():
    global thread_pool
    app.loop.run_in_executor(thread_pool, insertData)


def insertData():
    global tableName, listRecordStandby
    listRecordInsert = recordGenerator()
    
    print(listRecordInsert)

    """ if listRecordInsert:
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
        print("Whitout data") """


def recordGenerator():
    global setZonesId, setCamerasId, listDisaggregatedRecords
    recordAforo = {}
    listRecordAforo = []
    
    if listDisaggregatedRecords:
        for cameraId in setCamerasId:
            listFiltered_camId = [e for e in listDisaggregatedRecords if cameraId == e['camera_id']]
            if listFiltered_camId:
                for zoneId in setZonesId:
                    listcount_zoneId = [int(e["count"]) for e in listFiltered_camId if zoneId == e['zona_id']]
                    prom_count = numpy.mean(listcount_zoneId)
                    if numpy.isnan(prom_count):
                        prom_count = 0
                    if zoneId in recordAforo.keys():
                        recordAforo[zoneId] += round(prom_count)
                    else:
                        recordAforo[zoneId] = round(prom_count) 
        for zoneRec in recordAforo.keys():
            listRecordAforo.append(getInfoCam(zoneRec)+(recordAforo[zoneRec],))

    listDisaggregatedRecords.clear()
    setZonesId.clear()
    setCamerasId.clear()
    return listRecordAforo

def getInfoCam(zoneId):
    global jsonCamInfo
    now = datetime.now()
    date = now.strftime("%m/%d/%Y")
    time = now.strftime("%H:%M:%S")
    try:
        return(jsonCamInfo['id_cc'], date, time, zoneId, jsonCamInfo['zona'][zoneId])
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
