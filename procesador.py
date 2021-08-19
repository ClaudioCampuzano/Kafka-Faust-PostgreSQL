import psycopg2
import faust
from time import gmtime, strftime
from concurrent.futures import ThreadPoolExecutor

thread_pool = ThreadPoolExecutor(max_workers=1)

app = faust.App(
    "app1",
    broker='kafka://-:-',
    value_serializer='json',
)
test_topic = app.topic("-")
historial_cam = []
regCameras = set()

@app.agent(test_topic)
async def procesamiento(events):
    async for event in events:
        datosCamaras = {}
        aux = {}
        cnt = 0
        count = event["lc_person"]["count"].split("|")
        for i in event["lc_person"]["line_id"].split("|"):
            aux1 = {}
            if i != '':
                if int(i) == 0:
                    aux1['ins'] = count[cnt]
                else:
                    aux1['outs'] = count[cnt]
                aux[cnt] = aux1
                cnt += 1
        datosCamaras[event["lc_person"]["camera_id"]] = aux
        historial_cam.append(datosCamaras)
        regCameras.add(event["lc_person"]["camera_id"])

@app.timer(300.0)
async def my_periodic_task():
    app.loop.run_in_executor(thread_pool, insert_data)

def preProcesamiento(registrosStream):
    jirito = []
    for camId in regCameras:
        aux = list(e for e in registrosStream if camId in e)
        if len(aux) != 0:
            ins = 0
            out = 0
            for reg in aux:
                ins += int(reg[camId][0]['ins'])
                out += int(reg[camId][1]['outs'])
            jirito.append((camId, strftime("%Y/%m/%d", gmtime()),strftime("%H:%M:%S", gmtime()),"laCatunga","-", "SioKE",ins,out))
    return jirito

# Funciones de conexion a la Base de datos
param_dic = {
    "host": "-",
    "database": "-",
    "user": "-",
    "password": "-"
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    print("Connection successful")
    return conn


def insert_data():
    
    global historial_cam
    recordInsert = preProcesamiento(historial_cam)

    sqlQuery = """INSERT INTO test_dk_crack(id_cc, fecha, hora, acceso_id, nombre_comercial_acceso, piso, ins, outs) VALUES(%s,%s,%s,%s,%s,%s,%s,%s);"""
    try:
        conn = connect(param_dic)
        cur = conn.cursor()
        for i in recordInsert:
            cur.execute(sqlQuery, i)
            conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("PostgreSQL connection is closed")
            historial_cam=[]
