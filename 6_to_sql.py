import mysql.connector
import pandas as pd
from datetime import time
import json

DELIMITER = (",", '"', "\n") 
SQL_QUERY = "LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY '{}' ENCLOSED BY '{}' LINES TERMINATED BY '{}' IGNORE 1 LINES;" 
 
mydb = mysql.connector.connect(host="106.51.63.60", user="mahesh", password="mahesh_123", allow_local_infile=True) 
cursor = mydb.cursor() 
cursor.execute("use ushistoricaldb")
cursor.execute("SET GLOBAL local_infile=1")
 
filepath = r"D:\\github\\Daily_update\\28-3-QQQ\\active_df_put.csv" 
tableN = "qqq_put"

cursor.execute(SQL_QUERY.format(filepath, tableN, DELIMITER[0], DELIMITER[1], DELIMITER[2]))
mydb.commit()