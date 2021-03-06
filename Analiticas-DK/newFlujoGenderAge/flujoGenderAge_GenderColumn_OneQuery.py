# Informacion hacia PostgreSQL
import psycopg2
import faust
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from psycopg2 import extras
from psycopg2 import sql
import random
import datetime 
import numpy as np

thread_pool = ThreadPoolExecutor(max_workers=None)

param_dic = {
    "host": "192.168.0.127",
    "database": "dk_omia",
    "user": "postgres",
    "password": "Video2021$"
}
tableNameFlujo = 'ingreso_persona'
timeToUpload = 60.0

app = faust.App(
    "flujo",
    broker='kafka://34.227.94.165:9092',
    value_serializer='json',
)
main_topic = app.topic("roi")

with open("camarasInfo_flujo.json") as jsonFile:
    jsonCamInfo = json.load(jsonFile)
    jsonFile.close()

    
listDisaggregatedRecords = []
listRecordStandby = []
setCameraId = set()
currentTime = datetime.datetime.strptime('2000-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

@app.agent(main_topic)
async def streamUnbundler(events):
    global listDisaggregatedRecords, setCameraId, currentTime

    async for event in events:
        keysEvent = [*event]
        if 'lc_person' in keysEvent:
            currentTime = datetime.datetime.strptime(event["@timestamp"], '%Y-%m-%d %H:%M:%S')
            dictCamera = {}

            listCounts = [x for x in event['lc_person']['count'].split('|') if x]
            dictCamera['counter'] = listCounts

            subKeysEvent = [*event['lc_person']]

            for keyGender in ['males','females']:
                if keyGender in subKeysEvent:
                    genderList = [x for x in event['lc_person'][keyGender].split('|') if x]
                    dictCamera[keyGender] = genderList

                if keyGender+'_details' in subKeysEvent:
                    for keyAge in ['age_1_10','age_11_18','age_19_35','age_36_50','age_51_64','age_GTE_65']:
                        ageList = [x for x in event['lc_person'][keyGender+'_details'][keyAge].split('|') if x]
                        dictCamera[keyGender+'-'+keyAge] = ageList

            dictCamera['camera_id'] = event["camera_id"]
            setCameraId.add(dictCamera['camera_id'])
            listDisaggregatedRecords.append(dictCamera)


@app.timer(timeToUpload)
async def loadTopostgreSQL():
    global thread_pool
    app.loop.run_in_executor(thread_pool, insert_data)


def insert_data():
    global param_dic, listRecordStandby, tableNameFlujo
    recordFlujo = recordGenerator()

    if recordFlujo listRecordStandby:
        queryTextFlujo = "INSERT INTO {table}(id_cc, fecha, hora, acceso_id, nombre_comercial_acceso, piso, ins, outs, males, females, cnt_hombre_1_10, cnt_hombre_11_18,cnt_hombre_19_35,cnt_hombre_36_50,cnt_hombre_51_64,cnt_hombre_gt_65, cnt_mujer_1_10, cnt_mujer_11_18, cnt_mujer_19_35, cnt_mujer_36_50, cnt_mujer_51_64, cnt_mujer_gt_65) VALUES %s;"
        try:
            conn = connect(param_dic)
            if conn is None:
                raise ValueError('Error when trying to connect to the DB ...')
            cur = conn.cursor()

            sqlQueryFlujo = sql.SQL(queryTextFlujo).format(table=sql.Identifier(tableNameFlujo))
            extras.execute_values(cur, sqlQueryFlujo.as_string(cur), recordFlujo+listRecordStandby)            
            conn.commit()
            
            print("Inserting current data: "+str(cur.rowcount)+" records inserted successfully")
            if listRecordStandby:
                print("Of the above information, " +
                      str(len(listRecordStandby)) + " corresponds to old data")
                listRecordStandby = []
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            listRecordStandby += recordFlujo
            print("Saving information ("+str(len(recordFlujo))+" records) for future use (" +
                  str(len(listRecordStandby))+" total records in standby)")
        finally:
            if conn is not None:
                conn.close()
    else:
        print("Whitout data")


def recordGenerator():
    global setCameraId, listDisaggregatedRecords, jsonCamInfo
    listRecordFlujo = []
    
    if listDisaggregatedRecords:
        for camId in setCameraId:
            listFiltered = [e for e in listDisaggregatedRecords if camId == e['camera_id']]
            if listFiltered:
                income, outflows = 0, 0
                malesIn, femalesIn = 0,0
                ## ['age_1_10','age_11_18','age_19_35','age_36_50','age_51_64'][,'age_GTE_65'[in,out]
                cnt_malesIn, cnt_femalesIn = ([0]*6 for i in range(2))

                for record in listFiltered:
                    for index, count in enumerate(record['counter']):
                        if index % 2 == 0:
                            income += int(count)
                        else:
                            outflows += int(count)
                            
                    for keyGender in ['males','females']:
                        for index, count in enumerate(record[keyGender]):
                            if index % 2 == 0:  #Entradas
                                if keyGender == 'males':
                                    malesIn += int(count)
                                else:
                                    femalesIn += int(count)

                        for indexA, keyAge in enumerate(['age_1_10','age_11_18','age_19_35','age_36_50','age_51_64','age_GTE_65']):
                            nameKey = keyGender+'-'+keyAge
                            if nameKey in record:
                                for index, count in enumerate(record[nameKey]):
                                    if index % 2 == 0: #Entradas
                                        if keyGender == 'males':
                                            cnt_malesIn[indexA] += int(count)
                                        else:
                                            cnt_femalesIn[indexA] += int(count)
                listRecordFlujo.append(getInfoCam(camId)+tuple([income, outflows,malesIn,femalesIn])+tuple(cnt_malesIn)+tuple(cnt_femalesIn))

    listDisaggregatedRecords.clear()
    setCameraId.clear()
    return listRecordFlujo

def getInfoCam(camId):
    global jsonCamInfo, currentTime
    date = currentTime.strftime("%m/%d/%Y")
    time = currentTime.strftime("%H:%M:%S")
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

def reviewRatioGender(maleCnt, femaleCnt, cntTotal):
    femaleRatio = round((femaleCnt/(maleCnt + femaleCnt)) * cntTotal if int(cntTotal) else 0)
    maleRatio = round((maleCnt/(maleCnt + femaleCnt)) * cntTotal if int(cntTotal) else 0)

    if femaleRatio + maleRatio != cntTotal:
        dif = cntTotal - (femaleRatio + maleRatio)
        if (dif%2 == 0 and femaleRatio + int(dif/2) >= 0 and maleRatio + int(dif/2) >= 0):
            femaleRatio += int(dif/2)
            maleRatio += int(dif/2)
        else:
            if femaleRatio >= maleRatio:
                femaleRatio += dif
            else:
                maleRatio += dif
    return maleRatio, femaleRatio

def reviewRatioAgeGender(maleCnt, femaleCnt, cntTotalPerson):
    auxRatios= []
    totalGender=sum(maleCnt+femaleCnt)
    if int(cntTotalPerson):
        if totalGender:
            for count in maleCnt+femaleCnt:
                auxRatios.append(round((count/totalGender)*cntTotalPerson))
        else:
            auxRatios = [-1] * 12
    else:
        return [0]*6, [0]*6

    if -1 in auxRatios:
        if cntTotalPerson % 2 == 0:
            maleRatio = [0,0,round(cntTotalPerson/2),0,0,0]
            femaleRatios = [0,0,round(cntTotalPerson/2),0,0,0]
        else:
            if random.choice([0,1]) == 0:
                maleRatios = [0,0,cntTotalPerson,0,0,0]
                femaleRatios = [0] * 6
            else:
                femaleRatios = [0,0,cntTotalPerson,0,0,0]
                maleRatios = [0] * 6
    else:
        if sum(auxRatios) != cntTotalPerson:
            dif = cntTotalPerson - sum(auxRatios)
            if dif > 0:
                auxRatios[np.argmin(auxRatios)] += dif
            else:
                auxRatios = recusiveRevisor(auxRatios, dif)

    return auxRatios[0:6], auxRatios[6:12]

def recusiveRevisor(auxRatios, dif):
    indexMax = np.argmax(auxRatios)
    auxRatios[indexMax] += dif
    if auxRatios[indexMax] < 0:
        difR = auxRatios[indexMax]
        auxRatios[indexMax] = 0
        auxRatios = recusiveRevisor(auxRatios,difR)
    return auxRatios

def getInfoCam(camId):
    global jsonCamInfo, currentTime
    date = currentTime.strftime("%m/%d/%Y")
    time = currentTime.strftime("%H:%M:%S")
    try:
        return(jsonCamInfo['id_cc'], date, time, jsonCamInfo[str(camId)]['acceso_id'], jsonCamInfo[str(camId)]['nombre_comercial_acceso'], jsonCamInfo[str(camId)]['piso'])
    except:
        return("-1", date, time, "---", "---", "---")
