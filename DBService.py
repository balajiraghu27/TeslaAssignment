import cx_Oracle
import configparser
import logging
from datetime import datetime

config = configparser.RawConfigParser()
config.read('ConfigFiles')
DBServer = config.get('DBDetails', "DBServer")
DBPort = config.get('DBDetails', "DBPort")
DBServiceName = config.get('DBDetails', "DBServiceName")
DBUser = config.get('DBDetails', "DBUser")
DBPassword = config.get('DBDetails', "DBPassword")
JobStatusSuccess = config.get('General', "JobStatusSuccess")

dsn_tns = cx_Oracle.makedsn(DBServer, DBPort, service_name=DBServiceName)
connection = cx_Oracle.connect(user=DBUser, password=DBPassword, dsn=dsn_tns)
cursor = connection.cursor()


def createtables():
    # Application tracker table creation
    select_statement = "select count(1) from dba_tables where table_name = 'APPLICATION_STATUS_TRACKER'"
    cursor.execute(select_statement)
    results = cursor.fetchall()
    statstableexist = int(results[0][0])
    if statstableexist == 0:
        create_table = "CREATE TABLE APPLICATION_STATUS_TRACKER	(	" \
                       "APPLICATION_NAME VARCHAR2(30),	EXTRACT_START_TIME VARCHAR2(30)," \
                       "EXTRACT_END_TIME VARCHAR2(30),	JOB_START_TIME TIMESTAMP,	J" \
                       "OB_END_TIME TIMESTAMP,	RECORDS_PROCESSED NUMBER,	" \
                       "JOB_STATUS VARCHAR2(20),	ERROR_DESC VARCHAR2(300))"
        cursor.execute(create_table)
        logging.info(str(datetime.now()) + ": APPLICATION_STATUS_TRACKER table created successfully")
    else:
        logging.info(str(datetime.now()) + ": APPLICATION_STATUS_TRACKER table already exist ")
        # USGS Earthquoke Catalog table creation
    select_statement = "select count(1) from dba_tables where table_name = 'USGS_EARTHQUOKE_CATALOG'"
    cursor.execute(select_statement)
    results = cursor.fetchall()
    usgstableexist = int(results[0][0])
    if usgstableexist == 0:
        create_table = "CREATE TABLE USGS_EARTHQUOKE_CATALOG(ID VARCHAR2(100) ," \
                       "PLACE  VARCHAR2(300),MAGNITUDE DECIMAL(7,4)," \
                       "OCCURENCE_TIME TIMESTAMP,USGS_SYSTEM_UPDATED_TIME TIMESTAMP," \
                       "ROW_PROCESSED_TIME TIMESTAMP)"
        cursor.execute(create_table)
        logging.info(str(datetime.now()) + ": USGS_EARTHQUOKE_CATALOG table created successfully")
    else:
        logging.info(str(datetime.now()) + ": USGS_EARTHQUOKE_CATALOG table already exist ")


# Function to load Status table
def loadstatustable(errorrecord):
    cursor.prepare("INSERT INTO APPLICATION_STATUS_TRACKER VALUES (:1, :2, :3, :4, :5, :6, :7, :8)")
    cursor.execute(None, errorrecord)
    connection.commit()
    logging.info(str(datetime.now()) + ": Status updated successfully")


# Function to load USGS data into Catalog table
def loaddata(records):
    cursor.prepare("INSERT INTO USGS_EARTHQUOKE_CATALOG VALUES(:1, :2, :3, :4, :5, :6)")
    cursor.executemany(None, records)
    connection.commit()
    logging.info(str(datetime.now()) + ": Data load completed successfully")

# DB Duplicate deletion
def duplciatedeletion():
    cursor.prepare("DELETE from USGS_EARTHQUOKE_CATALOG where ( ID,ROW_PROCESSED_TIME) in "
                   "(SELECT  ID,ROW_PROCESSED_TIME from (select ID,ROW_PROCESSED_TIME, ROW_NUMBER() "
                   "OVER(PARTITION by ID order by ROW_PROCESSED_TIME desc) RNK from "
                   "USGS_EARTHQUOKE_CATALOG ) where RNK <>1)")
    cursor.execute(None)
    connection.commit()

# Extract Last Processed time for Delta
def extractLastProcessedTime():
    select_statement = "SELECT MAX(EXTRACT_END_TIME) FROM APPLICATION_STATUS_TRACKER where JOB_STATUS ='Finished' "
    cursor.execute(select_statement)
    lastprocessedtimestamp = cursor.fetchall()
    return lastprocessedtimestamp[0][0]


# EPOCH time conversion
def EpochConverter(time):
    return datetime.fromtimestamp(time / 1000)
