import faust
import json

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

app = faust.App(
    "ETL",
    broker='kafka://127.0.0.1:9092',
    value_serializer='json',
)

main_topic = app.topic("DS-events")
alert_topic = app.topic("alerts-omia")

thread_pool = ThreadPoolExecutor(max_workers=None)

async def disaggregatedAlertTopic(event):
    await alert_topic.send(value=event)

@app.agent(main_topic)
async def streamUnbundlerAlerts(events):
    global listDisaggregatedRecordsAlerts
    async for event in events:
        dictCamera = {}
        auxDict = {}
        listCounts = event["roi_person"]["count"].split("|")
        listMaxs = event["roi_person"]["max"].split("|")
        listMins = event["roi_person"]["min"].split("|")
        for roiType in event["roi_person"]["roi_id"].split("|"):
            if roiType != '':
                auxDict['camera_id'] = event['roi_person']['camera_id']
                auxDict['count'] = int(listCounts[int(roiType)])
                auxDict['object_class'] = 'Person'
        dictCamera['@timestamp'] = event['@timestamp']
        dictCamera['detections'] = auxDict
        await disaggregatedAlertTopic(dictCamera)

