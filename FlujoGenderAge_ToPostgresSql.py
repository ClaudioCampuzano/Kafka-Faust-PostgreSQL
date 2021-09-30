import psycopg2
import faust
import json
import pytz

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
tableNameFlujo = 'dev_visitantes_totales'
tableNameAtributo = 'dev_visitantes_totales_atributo'

timeToUpload = 60.0

app = faust.App(
    "flujo",
    broker='kafka://-:9092',
    value_serializer='json',
)
main_topic = app.topic("Ds-Jiro3")

with open("camarasInfo_Flujo.json") as jsonFile:
    jsonCamInfo = json.load(jsonFile)
    jsonFile.close()

listDisaggregatedRecords = []
listRecordStandby = []
setCameraId = set()
currentTime = ''

ANALITICA = ["roi_person", "lc_person"]


@app.agent(main_topic)
async def streamUnbundler(events):
    global listDisaggregatedRecords, setCameraId, jsonCamInfo, currentTime

    async for event in events:
        currentTime = event["@timestamp"]
        keysEvent = [*event]
        if ANALITICA[1] in keysEvent:
            dictCamera = {}
            listCounts = [x for x in event[ANALITICA[1]]['count'].split('|') if x]
            dictCamera['counter'] = listCounts
            dictCamera['camera_id'] = event["camera_id"]
            
            subKeysEvent = [*event[ANALITICA[1]]]
            if "males" in subKeysEvent and "females" in subKeysEvent:
                males = [x for x in event[ANALITICA[1]]["males"].split('|') if x]
                females = [x for x in event[ANALITICA[1]]["females"].split('|') if x]
                dictCamera['gender'] = {'males': males, 'females': females}
            
            for key in ['males_details', 'females_details']:
                if key in subKeysEvent:
                    age_1_10 = [x for x in event[ANALITICA[1]][key]["age_1_10"].split('|') if x]
                    age_11_18 = [x for x in event[ANALITICA[1]][key]["age_11_18"].split('|') if x]
                    age_19_35 = [x for x in event[ANALITICA[1]][key]["age_19_35"].split('|') if x]
                    age_36_50 = [x for x in event[ANALITICA[1]][key]["age_36_50"].split('|') if x]
                    age_51_64 = [x for x in event[ANALITICA[1]][key]["age_51_64"].split('|') if x]
                    age_GTE_65 = [x for x in event[ANALITICA[1]][key]["age_GTE_65"].split('|') if x]
                    dictCamera[key] = {'age_1_10': age_1_10, 'age_11_18': age_11_18, 'age_19_35': age_19_35,
                                       'age_36_50': age_36_50, 'age_51_64': age_51_64, 'age_GTE_65': age_GTE_65}
            listDisaggregatedRecords.append(dictCamera)
            setCameraId.add(event["camera_id"])


@app.timer(timeToUpload)
async def loadTopostgreSQL():
    global thread_pool
    app.loop.run_in_executor(thread_pool, insert_data)


def insert_data():

    global param_dic, listRecordStandby, tableNameFlujo, tableNameAtributo
    listRecordInsert = recordGenerator()
    print(listRecordInsert)

      if listRecordInsert[0] or listRecordStandby[0]:
        queryTextFlujo = "INSERT INTO {tableNameFlujo}(id_cc, fecha, hora, acceso_id, nombre_comercial_acceso, piso, ins, outs, registro_id) VALUES %s;"
        queryTextAtributo = "INSERT INTO {tableNameAtributo}(registro_id, tipo_acceso, cnt_hombre_1_10, cnt_hombre_11_18,cnt_hombre_19_35,cnt_hombre_36_50,cnt_hombre_51_64,cnt_hombre_gt_65, cnt_mujer_1_10, cnt_mujer_11_18, cnt_mujer_19_35, cnt_mujer_36_50, cnt_mujer_51_64, cnt_mujer_gt_65) VALUES %s;"

        try:
            conn = connect(param_dic)
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
    else:
        print("Whitout data")  


def recordGenerator():
    global setCameraId, listDisaggregatedRecords, currentTime
    listRecordFlujo = []
    listRecordAtributosIn = []
    listRecordAtributosOut = []
    if listDisaggregatedRecords:
        for camId in setCameraId:
            listFiltered = [e for e in listDisaggregatedRecords if camId == e['camera_id']]
            if listFiltered:
                ins, outs = 0, 0
                # 0-> Ins 1->Out
                cnt_males, cnt_females = list(), list()
                for i in range(0,2):
                    # 1_10, 11_18, 19_35, 36_50, 51_64, GT_65;
                    cnt_males.append([0] *6)
                    cnt_females.append([0] *6)
                    
                for record in listFiltered:
                    ins += int(record['counter'][0])
                    outs += int(record['counter'][1])
                    
                    for detailsGenderKey in ['males_details', 'females_details']:
                        if detailsGenderKey in record:
                            for typeAcess in range(0,2):
                                index = 0
                                for AgeKey in record[detailsGenderKey]:
                                    if detailsGenderKey == 'males_details':
                                        cnt_males[typeAcess][index] += int(record[detailsGenderKey][AgeKey][typeAcess])
                                    else:
                                        cnt_females[typeAcess][index] += int(record[detailsGenderKey][AgeKey][typeAcess])
                                    index += 1
            record_id = str(jsonCamInfo[str(camId)]['id_cc']) + "_" + str(camId) + "_" + "_".join(currentTime.split(" "))
            listRecordFlujo.append(getInfoCam(camId)+(ins, outs, record_id))
            listRecordAtributosIn.append((record_id, 'ins') + tuple(cnt_males[0]) + tuple(cnt_females[0]))
            listRecordAtributosOut.append((record_id, 'outs') + tuple(cnt_males[1]) + tuple(cnt_females[1]))
    listDisaggregatedRecords.clear()
    setCameraId.clear()
    return [listRecordFlujo,listRecordAtributosIn, listRecordAtributosOut]


def getInfoCam(camId):
    global jsonCamInfo
    now = datetime.now()
    date = now.strftime("%m/%d/%Y")
    time = now.strftime("%H:%M:%S")
    try:
        return(jsonCamInfo[str(camId)]['id_cc'], date, time, jsonCamInfo[str(camId)]['acceso_id'], jsonCamInfo[str(camId)]['nombre_comercial_acceso'], jsonCamInfo[str(camId)]['piso'])
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
