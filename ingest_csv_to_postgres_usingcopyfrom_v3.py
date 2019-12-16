import psycopg2
import re
import os.path
from datetime import date
import csv
import os
import shutil
from datetime import datetime

schemaname="raw"
tablename = "t_forecast_shipments"
connstring = "host=192.168.21.68 dbname=kelloggs_db user= password="
filename = "/data/s3data/STR/CSVFiles/ddpo_full_20181006.csv"
dataFolder = "/data/s3data/Forecast_Shipments/"
archiveFolder = "/data/s3data/Archive/Forecast_Shipments/"
# file_name="ddpo_full_20181006.csv"

'''
1. column names should not contain any postgres reserved words
2. column names should not contain any special characters (like ., # etc)

'''


def ingest_csv_to_postgres_usingcopyfrom(connstring, filename, tablename):

    table_def = []
    data_type = []
    my_csv_file = filename
    file_name = os.path.basename(filename)
    ingest_date = (date.today()).strftime("%Y-%m-%d")
    # d1 = ingest_date.strftime("%Y-%m-%d")
    # print(ingest_date)
    table_name = os.path.splitext(os.path.split(my_csv_file)[1])[0]
    # print(table_name)
    table_exists_check_statement="SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_schema = \'"+ schemaname +"\' AND table_name = \'"+tablename+"\' );"
    conn = psycopg2.connect(connstring)
    cur = conn.cursor()
    cur.execute(table_exists_check_statement)
    result=cur.fetchall()
    result_list=[x[0] for x in result]
    table_exists_flag=result_list[0]
    openFile = open(my_csv_file)
    cols = next(csv.reader(openFile))
    # print(cols)
    if table_exists_flag:
    	column_list_statement="select column_name from information_schema.columns c where table_schema= \'"+ schemaname +"\' AND table_name = \'"+tablename+"\' ;"
    	#print(column_list_statement)
    	cur.execute(column_list_statement)
    	result=cur.fetchall()
    	column_list=[x[0] for x in result]
    	#print (column_list)
    else:
    	#print (cols)
    	for row in cols:
    		row = re.sub(r'\W+', ' ', row.strip())
    		#print("first row", row)
    		#print("second row", row)
    		row = re.sub(r"\s+", '_', row)
    		#print("third row", row)
    		table_def.append(row)
    		data_type.append("varchar NULL")
    	table_def.append("file_name")
    	table_def.append("updated_date")
    	data_type.append("varchar NULL")
    	data_type.append("varchar NULL")
    	openFile.close()
    	#print(table_def)
    	#print(data_type)
    	create_statement = "CREATE TABLE IF NOT EXISTS " + schemaname+"."+tablename + " ("
    	i = 0
    	while i < len(table_def)-1:
        	create_statement = create_statement + table_def[i] + " " + data_type[i] + " ,"
        	i = i+1
    	create_statement = create_statement + table_def[i] + " " + data_type[i] + " )"
    	#print(create_statement)
    	conn = psycopg2.connect(connstring)
    	cur = conn.cursor()
    	result = cur.execute(create_statement)
    	print(create_statement)
    	conn.commit()
    	column_list = table_def
    	print("The column list in else part:", column_list)
    column_list.pop()
    column_list.pop()
    # print(column_list)
    sqlstatement="COPY " + schemaname+"."+tablename + "(" + ",".join(column_list) + ") FROM STDIN WITH CSV HEADER DELIMITER AS ',';"
    with open(my_csv_file, 'r', encoding="latin_1") as f:
        next(f)  # Skip the header row.
        """
        run the below next(f) only fior material master after confirming the first few rows are headers
        """
        #next(f)
        #next(f)
        #next(f)
        """
        uncomment till here, the skip portion only for Material Master
        """
        # print(f.readline())
        # for i, line in enumerate(f):
            # if i == 1321:
                # print(line)
        # cur.copy_from(f, tablename, sep=',' , columns=column_list)
        cur.copy_expert( sql=sqlstatement, file=f) # f, tablename, columns=column_list)
        # ",".join(myList )
    # sqlstatement="\\copy " + tablename + "(" + ",".join(column_list) + ") FROM '" + my_csv_file +"' CSV HEADER DELIMITER ',';"
    # print (sqlstatement)
    # cur.execute(sqlstatement) 
    conn.commit()
    cur.close()
    f.close()
    conn = psycopg2.connect(connstring)
    cur = conn.cursor()
    # update_statement = "UPDATE " + tablename + " SET updated_date = " + ingest_date + " , file_name = " + file_name + ";"
    update_statement = "UPDATE " + schemaname+"."+tablename + " SET updated_date = (%s) , file_name = (%s) WHERE file_name is null and updated_date is null"
    #cur.execute("UPDATE table_name SET update_column_name=(%s) WHERE ref_column_id_value = (%s)", ("column_name","value_you_want_to_update",));
    # print(update_statement)
    cur.execute(update_statement,(ingest_date, file_name))
    conn.commit()
    sqlstatement="SELECT COUNT(1) FROM "+schemaname+"."+tablename+" where file_name = (%s)"
    #print(sqlstatement)
    cur.execute(sqlstatement,(file_name,))
    count_rows_inserted=cur.fetchall()
    """
    added this to automatically grant table access to suman and ayan. should delete this in the original script
    """

    """
    sqlstatement="grant SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw TO \"ayan.putatunda\""
    cur.execute(sqlstatement)
    conn.commit()
    sqlstatement="grant SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw TO \"suman.gangopadhyay\""
    cur.execute(sqlstatement)
    conn.commit()
    sqlstatement="grant SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw TO \"sowmya.jonnalagadda\""
    cur.execute(sqlstatement)
    conn.commit()
    """
    cur.close()
    conn.close()
    print("File Inserted to table. The inserted count is" , count_rows_inserted)
    return("SUCCESS")

def archiverenamefiles (srcfolder, destfolder,file_name = None):

    os.makedirs(destfolder, exist_ok = True)

    if file_name is not None:
        base, extension = os.path.splitext(file_name)
        update_filename = file_name.replace(extension, "")+"_Ingested_"+datetime.now().date().strftime("%m/%d/%Y").replace("/", "")+extension
        updated_name = destfolder + "/" + update_filename
        shutil.move(srcfolder+"/" + file_name, updated_name)
        print("This file is archived",file_name)
        return ("SUCCESS")

    for root, dirs, files in os.walk(srcfolder):
        for filename in files:
            # current_name = os.path.join(os.path.abspath(root), filename)
            base, extension = os.path.splitext(filename)
            update_filename = filename.replace(extension, "")+"_Ingested_"+datetime.now().date().strftime("%m/%d/%Y").replace("/", "")+extension
            updated_name = destfolder + "/" + update_filename
            # print current_name
            # print updated_name
            shutil.move(srcfolder+"/" + filename, updated_name)  
            print("This file is archived",filename)
        return("SUCCESS")        
    return("FAIL")    


"""
the below will consider  all the files in the files list provided in the file_list to process/ingest and archive
"""


#file_List =['Locations.csv']
file_List=['Historical_Transit_Times.csv']
'''
for dirName, subdirList, fileList in os.walk(dataFolder):
    for fname in fileList:
        if fname in file_List:
            file_name = fname
            filename = dataFolder+fname
            print("processing file",filename)
            ingest_csv_to_postgres_usingcopyfrom(connstring, filename, tablename)
            #archiverenamefiles(dataFolder, archiveFolder,file_name)

'''
"""
the below will consider  all the files in the data folder to process/ingest and archive
"""

for dirName, subdirList, fileList in os.walk(dataFolder):
    for fname in fileList:
        file_name = fname
        filename = dataFolder+fname
        print("processing file",filename)
        ingest_csv_to_postgres_usingcopyfrom(connstring, filename, tablename)
        #archiverenamefiles(dataFolder, archiveFolder,file_name)    


