import faust
import datetime
import psycopg2
from psycopg2 import extras
from psycopg2 import sql
import json
from concurrent.futures import ThreadPoolExecutor


thread_pool = ThreadPoolExecutor(max_workers=None)

param_postgreSQL = {
    "host": "-",
    "database": "postgres",
    "user": "-",
    "password": "-"
}
tableName = 'dev_visitantes_totales'

timeToUpload = 25.0

app = faust.App(
    "permanencia2",
    broker="kafka://-:9092",
    value_serializer="json",
)
main_topic = app.topic("analytics-jiro1")

tablaEventos = {}
currentTime = datetime.datetime.strptime(
    '2020-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
listRecordStandby = []

ANALITICA = ["roi_person"]

with open("camInfo_permanencia.json") as jsonFile:
    jsonCamInfo = json.load(jsonFile)
    jsonFile.close()


@app.agent(main_topic)
async def dataEntry(events):
    global tablaEventos, currentTime
    async for event in events:
        currentTime = datetime.datetime.strptime(
            event["@timestamp"], '%Y-%m-%d %H:%M:%S')
        keysEvent = [*event]
        if ANALITICA[0] in keysEvent:
            subKeysEvent = [*event[ANALITICA[0]]]
            if 'ids' in subKeysEvent:
                # lista de ids limpia sin espacio vacio o nulos
                list_ID = set(
                    str(event["camera_id"])+"-"+x for x in event[ANALITICA[0]]["ids"].split('|') if x)
                for id in list_ID:
                    if id in tablaEventos:
                        tablaEventos[id].append(event["@timestamp"])
                        if len(tablaEventos[id]) > 2:
                            tablaEventos[id].pop(1)
                    else:
                        tablaEventos[id] = list()
                        tablaEventos[id].append(event["@timestamp"])


@app.timer(timeToUpload)
async def loadToBD():
    global thread_pool
    app.loop.run_in_executor(thread_pool, insert_data)


def insert_data():
    global tableName, param_postgreSQL, listRecordStandby, currentTime

    listRecordInsert = recordGenerator(currentTime)
    print(listRecordInsert)


     if listRecordInsert:
        queryText = "INSERT INTO {table}(id_cc, fecha, hora, Zona, Zona_id, tpromedio) VALUES %s;"
        try:
            conn = connect(param_postgreSQL)
            if conn is None:
                raise ValueError('Error when trying to connect to the DB ...')
            cur = conn.cursor()

            sqlQuery = sql.SQL(queryText).format(
                table=sql.Identifier(tableName))
            extras.execute_values(cur, sqlQuery.as_string(
                cur), listRecordInsert+listRecordStandby)
            conn.commit()

            print("Inserting current data: "+str(cur.rowcount) +
                  " records inserted successfully")
            if len(listRecordStandby) != 0:
                print("Of the above information, " +
                      str(len(listRecordStandby)) + " corresponds to old data")
                listRecordStandby = []
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            listRecordStandby += listRecordInsert
            print("Saving information ("+str(len(listRecordInsert))+" records) for future use (" +
                  str(len(listRecordStandby))+" total records in standby)")
        finally:
            if conn is not None:
                conn.close()


def recordGenerator(cTime):
    global tablaEventos
    listRecord = []
    dicTimes = {}

    for eventosId in tablaEventos:
        tInicial = datetime.datetime.strptime(
            tablaEventos[eventosId][0], '%Y-%m-%d %H:%M:%S')
        tFinal = datetime.datetime.strptime(
            tablaEventos[eventosId][1], '%Y-%m-%d %H:%M:%S')
        timeInScene = (tFinal-tInicial).total_seconds()
        camId = eventosId.split("-")[0]
        if camId in dicTimes:
            dicTimes[camId] += timeInScene
        else:
            dicTimes[camId] = timeInScene
        tablaEventos[eventosId].clear()

    for camId in dicTimes:
        listRecord.append(getInfoCam(camId, cTime) +
                          (str(dicTimes[camId]).split(".")[0],))
    return listRecord


def getInfoCam(camId, cTime):
    global jsonCamInfo
    date = cTime.strftime("%m/%d/%Y")
    time = cTime.strftime("%H:%M:%S")
    try:
        return(jsonCamInfo[str(camId)]['id_cc'], date, time, jsonCamInfo[str(camId)]['zona'], jsonCamInfo[str(camId)]['zona_id'])
    except:
        return("-1", date, time, "---", "---", "---")


def connect(params_dic):
    conn = None
    try:
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(end=" ")
    return conn
