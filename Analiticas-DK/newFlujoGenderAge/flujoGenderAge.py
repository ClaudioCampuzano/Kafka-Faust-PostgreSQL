# Informacion hacia PostgreSQL
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
tableNameFlujo = 'dev_visitantes_totales'
tableNameAtributo = 'dev_visitantes_totales_atributo'

timeToUpload = 15.0

app = faust.App(
    "flujo",
    broker='kafka://34.227.94.165:9092',
    value_serializer='json',
)
main_topic = app.topic("roi")

with open("camarasInfo_flujo.json") as jsonFile:
    jsonCamInfo = json.load(jsonFile)
    jsonFile.close()
    
OnlyGender = True

listDisaggregatedRecords = []
listRecordStandby = []
setCameraId = set()

@app.agent(main_topic)
async def streamUnbundler(events):
    global listDisaggregatedRecords, setCameraId, OnlyGender

    async for event in events:
        keysEvent = [*event]
        if 'lc_person' in keysEvent:
            dictCamera = {}

            listCounts = [x for x in event['lc_person']['count'].split('|') if x]
            dictCamera['counter'] = listCounts

            subKeysEvent = [*event['lc_person']]
            
            if OnlyGender:
                for keyGender in ['males','females']:
                    if keyGender in subKeysEvent:
                        genderList = [x for x in event['lc_person'][keyGender].split('|') if x]
                        dictCamera[keyGender] = genderList
            else:
                for keyGender in ['males_details', 'females_details']:
                    if keyGender in subKeysEvent:
                        for keyAge in ['age_1_10','age_11_18','age_19_35','age_36_50','age_51_64','age_GTE_65']:
                            ageList = [x for x in event['lc_person'][keyGender][keyAge].split('|') if x]
                            dictCamera[keyGender.split('_')[0]+'-'+keyAge] = ageList

            dictCamera['camera_id'] = event["camera_id"]
            setCameraId.add(dictCamera['camera_id'])
            listDisaggregatedRecords.append(dictCamera)
            

@app.timer(timeToUpload)
async def loadTopostgreSQL():
    global thread_pool
    app.loop.run_in_executor(thread_pool, insert_data)


def insert_data():
    global param_dic, listRecordStandby, tableNameFlujo, tableNameAtributo
    listRecordInsert = recordGenerator()
    
    if listRecordInsert[0] or listRecordInsert[1] or listRecordStandby[0] or listRecordStandby[1]:
        queryTextFlujo = "INSERT INTO {table}(id_cc, fecha, hora, acceso_id, nombre_comercial_acceso, piso, ins, outs, registro_id) VALUES %s;"
        if OnlyGender:
            queryTextAtributo = "INSERT INTO {table}(registro_id, tipo_acceso, cnt_hombre_1_10, cnt_hombre_11_18,cnt_hombre_19_35,cnt_hombre_36_50,cnt_hombre_51_64,cnt_hombre_gt_65, cnt_mujer_1_10, cnt_mujer_11_18, cnt_mujer_19_35, cnt_mujer_36_50, cnt_mujer_51_64, cnt_mujer_gt_65) VALUES %s;"
        else:
            queryTextAtributo = "INSERT INTO {table}(registro_id, tipo_acceso, cnt_hombre, cnt_mujer) VALUES %s;"

        try:
            conn = connect(param_dic)
            if conn is None:
                raise ValueError('Error when trying to connect to the DB ...')
            cur = conn.cursor()

            sqlQueryFlujo = sql.SQL(queryTextFlujo).format(table=sql.Identifier(tableNameFlujo))
            sqlQueryAtributo = sql.SQL(queryTextAtributo).format(table=sql.Identifier(tableNameAtributo))

            extras.execute_values(cur, sqlQueryFlujo.as_string(cur), listRecordInsert[0]+listRecordStandby[0])
            extras.execute_values(cur, sqlQueryAtributo.as_string(cur), listRecordInsert[1]+listRecordStandby[1])

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
    global setCameraId, listDisaggregatedRecords, jsonCamInfo, OnlyGender
    listRecordFlujo = []
    listRecordAtributosIn = []
    listRecordAtributosOut = []

    if listDisaggregatedRecords:
        for camId in setCameraId:
            listFiltered = [e for e in listDisaggregatedRecords if camId == e['camera_id']]
            if listFiltered:
                income, outflows = 0, 0
                malesIn, malesOut, femalesIn, femalesOut = 0,0,0,0
                ## ['age_1_10','age_11_18','age_19_35','age_36_50','age_51_64'][,'age_GTE_65'[in,out]
                cnt_malesIn, cnt_femalesIn = ([0]*6 for i in range(2))
                cnt_malesOut, cnt_femalesOut = ([0]*6 for i in range(2))

                for record in listFiltered:
                    for index, count in enumerate(record['counter']):
                        if index % 2 == 0:
                            income += int(count)
                        else:
                            outflows += int(count)
                            
                    if OnlyGender:
                        for keyGender in ['males','females']:
                            for index, count in enumerate(record[keyGender]):
                                if index % 2 == 0:
                                    if keyGender == 'males':
                                        malesIn += int(count)
                                    else:
                                        femalesIn += int(count)
                                else:
                                    if keyGender == 'males':    
                                        malesOut += int(count)
                                    else:
                                        femalesOut += int(count)
                    else:
                        for keyGender in ['males', 'females']:
                            for indexA, keyAge in enumerate(['age_1_10','age_11_18','age_19_35','age_36_50','age_51_64','age_GTE_65']):
                                nameKey = keyGender+'-'+keyAge
                                if nameKey in record:
                                    for index, count in enumerate(record[nameKey]):
                                        if index % 2 == 0: #Entradas
                                            if keyGender == 'males':
                                                cnt_malesIn[indexA] += int(count)
                                            else:
                                                cnt_femalesIn[indexA] += int(count)
                                        else:              #Salidas
                                            if keyGender == 'males':
                                                cnt_malesOut[indexA] += int(count)
                                            else:
                                                cnt_femalesOut[indexA] += int(count)
                    
                
                
                now = datetime.now()
                currentTime = now.strftime("%m/%d/%Y_%H:%M:%S")
                record_id = str(jsonCamInfo['id_cc']) + "_" + str(camId) + "_" + str(currentTime)

                listRecordFlujo.append(getInfoCam(camId)+(income, outflows, record_id))
                if OnlyGender:
                    
                    malesInRatio,femalesInRatio = reviewRatio(malesIn, femalesIn, income)
                    malesOutRatio,femalesOutRatio = reviewRatio(malesOut, femalesOut, outflows)
                    
                    listRecordAtributosIn.append((record_id,'ins') + (malesInRatio, femalesInRatio))
                    listRecordAtributosOut.append((record_id,'outs') + (malesOutRatio,femalesOutRatio))
                else:
                    listRecordAtributosIn.append((record_id,'ins') + tuple(cnt_malesIn) + tuple(cnt_femalesIn))
                    listRecordAtributosOut.append((record_id,'outs') + tuple(cnt_malesOut) + tuple(cnt_femalesOut))
    
    listDisaggregatedRecords.clear()
    setCameraId.clear()
    return [listRecordFlujo,listRecordAtributosIn+listRecordAtributosOut]

def reviewRatio(maleCnt, femaleCnt, cntTotal):
    femaleRatio = round((femaleCnt/(maleCnt + femaleCnt)) * cntTotal)
    maleRatio = round((maleCnt/(maleCnt + femaleCnt)) * cntTotal)
                        
    if femaleRatio + maleRatio != cntTotal:
        dif = cntTotal - (femaleRatio + maleRatio)
        femaleRatio += dif
        
    return maleRatio, femaleRatio

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
