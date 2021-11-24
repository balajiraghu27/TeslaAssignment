import sys
from datetime import datetime

import DBService
import requests.exceptions
import ApiCall
import logging
import configparser


def runprocess(ExtractStartTime, ExtractEndTime):
    starttime = datetime.now()
    logging.info(str(starttime) + ": USGS EarthQuake Analysis Application started ")
    # Retrieving Server details from Config File
    config = configparser.RawConfigParser()
    config.read('ConfigFiles')
    countlink = config.get('USGSServerDetails', "countlink")
    datalink = config.get('USGSServerDetails', "datalink")
    USGSLimit = int(config.get('General', "USGSLimit"))
    ApplicationName = config.get('General', "ApplicationName")
    JobStatusSuccess = config.get('General', "JobStatusSuccess")
    JobStatusFailure = config.get('General', "JobStatusFailure")
    records = []

    try:
        # Api call to get Counts
        count = ApiCall.getcount(countlink, ExtractStartTime, ExtractEndTime)

        logging.info(str(datetime.now()) + ": Total records to be processed is " + str(count))
        iterations = (count // 20000)  # Max limit allowed by USGS
        # APi call to get the data and row format
        logging.info(str(datetime.now()) + ": Extracting the data from Api in batches of " + str(USGSLimit)
                     + ". Total batches will be " + str(iterations + 1))
        for x in range(iterations + 1):
            logging.info(str(datetime.now()) + ": Extracting data batch: " + str(x + 1))
            DataIterations = ApiCall.getdata(datalink, (x * USGSLimit) + 1, ExtractStartTime, ExtractEndTime, USGSLimit)
            for Data in (DataIterations["features"]):
                record = (Data["id"], Data["properties"]["place"], Data["properties"]["mag"],
                          DBService.EpochConverter(Data["properties"]["time"]),
                          DBService.EpochConverter(Data["properties"]["updated"]), datetime.now())
                records.append(record)
        # DB Table Creation
        DBService.createtables()

        # DB call for Insert
        DBService.loaddata(records)
        DBService.duplciatedeletion()
        # DB load Status Success
        current_time = datetime.now()
        logging.info(str(current_time) + ": Data load completed successfully for the period between " +
                     str(ExtractStartTime) + " and " + str(ExtractEndTime))
        statusrecord = (ApplicationName, ExtractStartTime, ExtractEndTime, starttime,
                        current_time, count, JobStatusSuccess, None)
        DBService.loadstatustable(statusrecord)

    except requests.exceptions.RequestException as e:
        current_time = datetime.now()
        logging.error(str(current_time) + ": Error while connecting API. Error description: " + str(e))
        # Load error data to DB
        statusrecord = (ApplicationName, ExtractStartTime, ExtractEndTime, starttime,
                        current_time, 0, JobStatusFailure, str(e))
        DBService.loadstatustable(statusrecord)
        raise SystemExit(e)
    except:
        e = sys.exc_info()
        current_time = datetime.now()
        logging.error(str(current_time) + ": Error while running the application. Error description: " + str(e))
        # Load error data to DB
        statusrecord = (ApplicationName, ExtractStartTime, ExtractEndTime, starttime,
                        current_time, 0, JobStatusFailure, str(e))
        DBService.loadstatustable(statusrecord)
        raise SystemExit(e)
