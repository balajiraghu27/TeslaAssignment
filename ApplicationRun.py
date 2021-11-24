from datetime import datetime
import schedule
import DBService
import logging
import time
import ExtractAndLoad

logging.basicConfig(filename='application.log', level=logging.INFO)
"""
This Application reads the USGS Earthquake and loads the information into USGS_EARTHQUOKE_CATALOG table and 
the application stats will be loaded into APPLICATION_STATUS_TRACKER table.
"""
ExtractStartTime = input("Enter Extract Start Time in YYYY-MM-DDTHH:MI:SS or YYYY-MM-DD format \n")
ExtractEndTime = input("Enter Extract Start Time in YYYY-MM-DDTHH:MI:SS or YYYY-MM-DD format \n")
ExtractAndLoad.runprocess(ExtractStartTime, ExtractEndTime)


# Function to start the incremental process
def deltaprocess():
    logging.info(str(datetime.now()) + ": Scheduler started ")
    ExtractAndLoad.runprocess(DBService.extractLastProcessedTime(), datetime.now().isoformat())


# Scheduler Configuration
schedule.every().day.at("10:00").do(deltaprocess)

while True:
    # Checks whether a scheduled task
    # is pending to run or not
    schedule.run_pending()
    time.sleep(10)
