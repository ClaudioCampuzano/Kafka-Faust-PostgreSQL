#### Procesamiento de los mensajes de kafka provinientes de los appliances, con informacion de aforo para su deposito en SQL Server
import faust
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import numpy as np
import pyodbc

thread_pool = ThreadPoolExecutor(max_workers=None)

app = faust.App(
    "ETL",
    broker="kafka://:9092",
    value_serializer="json",
)
main_topic = app.topic("analytics-omia")

set_camera_id = set()
list_disaggregated = list()

ANALITICA = ["roi_person", "lc_person"]

driver = '/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.8.so.1.1'
server = ''
database = ''
username = ''
password = ''
tableName = ''


@app.agent(main_topic)
async def streamRoiUnbundler(events):
    global set_camera_id, list_disaggregated
    async for event in events:
        keysEvent = [*event]
        dictReg = {}
        dictReg["@timestamp"] = event["@timestamp"]

        if ANALITICA[0] in keysEvent:
            list_count = event[ANALITICA[0]]["count"].split('|')
            list_max = event[ANALITICA[0]]["max"].split('|')
            list_min = event[ANALITICA[0]]["min"].split('|')
            list_roiID = event[ANALITICA[0]]["roi_id"].split('|')
            set_camera_id.add(event[ANALITICA[0]]["camera_id"])

            dictReg["frame_init"] = int(event[ANALITICA[0]]["frame_init"])
            dictReg["frame_fin"] = int(event[ANALITICA[0]]["frame_fin"])
            dictReg["period"] = int(event[ANALITICA[0]]["period"])
            dictReg["analytic"] = int(event[ANALITICA[0]]["analytic"])
            dictReg["obj_type"] = int(event[ANALITICA[0]]["obj_type"])
            dictReg["camera_id"] = int(event[ANALITICA[0]]["camera_id"])
            for roi_type in list_roiID:
                if roi_type != '':
                    dictPerCam = {}
                    dictPerCam["roi_id"] = int(roi_type)
                    dictPerCam["count"] = int(list_count[int(roi_type)])
                    dictPerCam["max"] = int(list_max[int(roi_type)])
                    dictPerCam["min"] = int(list_min[int(roi_type)])
                    list_disaggregated.append({**dictReg, **dictPerCam})


@app.timer(60.0)
async def loadToSqlBD():
    global set_camera_id, thread_pool, list_disaggregated
    app.loop.run_in_executor(thread_pool, insertData,
                             list_disaggregated, set_camera_id)


def insertData(listRecords, setCamerasId):
    global list_disaggregated, driver, server, database, username, password, tableName
    list_record_to_insert = recordGenerator(listRecords, setCamerasId)
    list_disaggregated = []

    if list_record_to_insert:
        try:
            conn = connect([driver, server, database, username, password])
            if conn is None:
                raise ValueError('Error when trying to connect to the DB ...')
            cur = conn.cursor()
            sqlQuery = f"INSERT INTO {tableName} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            try:
                conn.autocommit = False
                cur.fast_executemany = True
                cur.executemany(sqlQuery,list_record_to_insert)
            except pyodbc.DatabaseError as err:
                print(err)
                conn.rollback()
            else:
                print("Payload upload")
                conn.commit()
            finally:
                conn.autocommit = True
        except Exception as e:
            print(e)
        finally:
            if conn is not None:
                conn.close()
    else:
        print("Whitout data")


def recordGenerator(listRecords, setCamerasId):
    list_record = []
    if(len(listRecords) != 0):
        for cameraId in setCamerasId:
            list_filtered = [e for e in listRecords if str(
                cameraId) == str(e['camera_id'])]
            if list_filtered:
                dateTimeObj = datetime.strptime(list_filtered[-1]["@timestamp"], '%Y-%m-%d %H:%M:%S')
                bin_year = int(dateTimeObj.strftime("%Y"))
                bin_month = int(dateTimeObj.strftime("%m"))
                bin_day = int(dateTimeObj.strftime("%d"))
                bin_hour = int(dateTimeObj.strftime("%H"))
                bin_min = int(dateTimeObj.strftime("%M"))
                bin_time = dateTimeObj.strftime("%Y-%m-%d %H:%M:00.000")
                analytic = list_filtered[0]["analytic"]
                roi_id = list_filtered[0]["roi_id"]
                obj_type = list_filtered[0]["obj_type"]

                list_count = [e["count"] for e in list_filtered]
                avg_count = int(np.mean(np.array(list_count)))

                list_max = [e["max"] for e in list_filtered]
                _max = int(np.amax(np.array(list_max)))

                list_min = [e["min"] for e in list_filtered]
                _min = int(np.amin(np.array(list_min)))

                list_frame_init = [e["frame_init"] for e in list_filtered]
                frame_init = int(np.amin(np.array(list_frame_init)))

                list_frame_fin = [e["frame_fin"]for e in list_filtered]
                frame_fin = int(np.amax(np.array(list_frame_fin)))

                list_record.append((bin_year, bin_month, bin_day, bin_hour,
                                   bin_min, cameraId, analytic, roi_id, obj_type,
                                   bin_time, 'Minuto', -1, 'DeployOnDemand', _min,
                                   _max, avg_count, 0, frame_fin - frame_init, 0,
                                   0, 0))
    return list_record

def connect(params):
    conn = None
    try:
        conn = pyodbc.connect('driver={%s};server=%s;database=%s;uid=%s;pwd=%s' %(params[0], params[1], params[2], params[3], params[4]))
    except pyodbc.Error as e:
        print(e, "error")
    return conn
