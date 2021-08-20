import psycopg2
import faust
import pandas as pd
import json
import pytz

from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

param_dic = {
    "host": "-",
    "database": "-",
    "user": "-",
    "password": "-"
}

app = faust.App(
    "ETL",
    broker='kafka://-:9092',
    value_serializer='json',
)
test_topic = app.topic("-")

thread_pool = ThreadPoolExecutor(max_workers=1)

with open("camaras_info.json") as jsonFile:
    jsonCamInfo = json.load(jsonFile)
    jsonFile.close()

listDisaggregatedRecords = []
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


@app.crontab('0 5 * * *', timezone=guayaquil)
async def EveryDay_at_5_am():
    global thread_pool
    app.loop.run_in_executor(thread_pool, generateCSV)


def connect(params_dic):
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    #print("Connection successful PostgreSQL")
    return conn


def insert_data():

    global listDisaggregatedRecords, param_dic
    listRecordInsert = recordGenerator(listDisaggregatedRecords)
    listDisaggregatedRecords = []

    if len(listRecordInsert) != 0:
        print("Insertando data...")
        sqlQuery = """INSERT INTO test_dk_crack(id_cc, fecha, hora, acceso_id, nombre_comercial_acceso, piso, ins, outs) VALUES(%s,%s,%s,%s,%s,%s,%s,%s);"""
        try:
            conn = connect(param_dic)
            cur = conn.cursor()
            for record in listRecordInsert:
                cur.execute(sqlQuery, record)
                conn.commit()
            print(str(len(listRecordInsert))+" record inserted successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
    else:
        print("Sin data")


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


def postgresql_to_dataframe(conn, select_query, column_names):
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    cursor.close()

    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    return df


def generateCSV():
    global param_dic
    conn = connect(param_dic)
    yesterday = (datetime.now() - timedelta(1))
    column_names = ["rd_id", "id_cc", "fecha", "hora", "acceso_id",
                    "nombre_comercial_acceso", "piso", "ins", "outs", ]
    try:
        df = postgresql_to_dataframe(
            conn, "select * from test_dk_crack where fecha='"+yesterday.strftime("%m/%d/%Y")+"'", column_names)
        df.to_csv('dk_'+yesterday.strftime("%m_%d_%Y")+'.csv', index=False)
        print("CSV generated")

    except (Exception) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


