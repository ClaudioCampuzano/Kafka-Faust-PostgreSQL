# Procesamiento de los mensajes de kafka proviniente de los appliances, con informacion de flujo para su deposito en PostgreSQL
import psycopg2
import faust
import json
import pytz

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from psycopg2 import extras
from psycopg2 import sql

param_dic = {
    "host": "192.168.0.127",
    "database": "dk_omia",
    "user": "postgres",
    "password": "Video2021$"
}
tableName='dev_visitantes_totales'

app = faust.App(
    "ETL",
    broker='kafka://127.0.0.1:9092',
    value_serializer='json',
)
test_topic = app.topic("analytics-omia")

thread_pool = ThreadPoolExecutor(max_workers=None)

with open("camaras_info.json") as jsonFile:
    jsonCamInfo = json.load(jsonFile)
    jsonFile.close()

listDisaggregatedRecords = []
listRecordStandby = []
setCameraId = set()

guayaquil = pytz.timezone('America/Guayaquil')


@app.agent(test_topic)
async def streamUnbundler(events):
    global listDisaggregatedRecords, setCameraId
    async for event in events:
        dictCamera = {}
        auxDict = {}
        listCounts = event["lc_person"]["count"].split("|")
        for lineType in event["lc_person"]["line_id"].split("|"):
            if lineType != '':
                if int(lineType) == 0:  # Ingreso
                    auxDict['ins'] = int(listCounts[int(lineType)])
                else:  # Salida
                    auxDict['outs'] = int(listCounts[int(lineType)])
        dictCamera[event["lc_person"]["camera_id"]] = auxDict
        listDisaggregatedRecords.append(dictCamera)
        setCameraId.add(event["lc_person"]["camera_id"])


@app.timer(300.0)
async def loadTopostgreSQL():
    global thread_pool
    app.loop.run_in_executor(thread_pool, insert_data)

def connect(params_dic):
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(end=" ")
    #print("Connection successful PostgreSQL")
    return conn


def insert_data():

    global listDisaggregatedRecords, param_dic, listRecordStandby, tableName
    listRecordInsert = recordGenerator(listDisaggregatedRecords)
    listDisaggregatedRecords = []

    if len(listRecordInsert) != 0 or len(listRecordStandby) != 0:
        queryText = "INSERT INTO {table}(id_cc, fecha, hora, acceso_id, nombre_comercial_acceso, piso, ins, outs) VALUES %s;"
        try:
            conn = connect(param_dic)
            if conn is None:
                raise ValueError('Error when trying to connect to the DB ...')
            cur = conn.cursor()

            sqlQuery = sql.SQL(queryText).format(table=sql.Identifier(tableName))
            extras.execute_values(cur, sqlQuery.as_string(cur), listRecordInsert+listRecordStandby)
            conn.commit()

            print("Inserting current data: "+str(cur.rowcount)+" records inserted successfully")
            if len(listRecordStandby)!= 0:
                print("Of the above information, "+ str(len(listRecordStandby)) +" corresponds to old data") 
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            listRecordStandby += listRecordInsert
            print("Saving information ("+str(len(listRecordInsert))+" records) for future use ("+str(len(listRecordStandby))+" total records in standby)")
        finally:
            if conn is not None:
                conn.close()
    else:
        print("Whitout data")

def recordGenerator(recordStreams):
    global setCameraId
    listRecord = []
    if len(recordStreams) != 0:
        for camId in setCameraId:
            listFiltered = list(e for e in recordStreams if camId in e)
            if len(listFiltered) != 0:
                ins = 0
                outs = 0
                for record in listFiltered:
                    ins += int(record[camId]['ins'])
                    outs += int(record[camId]['outs'])
                listRecord.append(getInfoCam(camId)+(ins, outs))
    return listRecord


def getInfoCam(camId):
    global jsonCamInfo
    now = datetime.now()
    date = now.strftime("%m/%d/%Y")
    time = now.strftime("%H:%M:%S")
    try:
        return(jsonCamInfo[str(camId)]['id_cc'], date, time, jsonCamInfo[str(camId)]['acceso_id'], jsonCamInfo[str(camId)]['nombre_comercial_acceso'], jsonCamInfo[str(camId)]['piso'])
    except:
        return("-1", date, time, "---", "---", "---")
