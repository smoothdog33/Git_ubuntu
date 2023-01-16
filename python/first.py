import json
import os
import pandas as pd
import datetime
from datetime import datetime




import psycopg2
pgconn = psycopg2.connect(
    host= 'localhost',
    user = 'dev_test1',
    password = 'ayan',
    database = 'testing')
pgcursor = pgconn.cursor()
print(pgcursor.execute('select * from public."max_dates" where run_id in (select max(run_id) from public.max_dates) '))  
result = pgcursor.fetchall()     
max_a_time= result [0][1]
max_m_time= result [0][2]     
max_c_time= result [0][3]   
run_id= result [0][4] + 1                  
                                
#create a function that accepts 2 input arguments 
# 1: the "input_directory": the path for which we want to collect the properties and attributes for all the files in it
# 2:the "output_file_name": the json file where the entries of the files and attributes will be saved
#1: insert input directory here

def file_inventory_creator(input_directory,output_file_name,max_a_time,max_m_time,max_c_time,run_id):
        for root, dirs, files in os.walk(input_directory):
            for file in files:
                try:
                    path_creator = os.path.join(root,file)
                    file_abs_path = os.path.abspath(path_creator)
                    #old_abs_path = check_abs_path_func(output_file_name,file_abs_path)
                    #print(old_abs_path)
                    #print(file_abs_path)

                    #print(g)
                    # 4: collect properties and attributes
                    size = os.stat(file_abs_path)
                    #print( "the size of the file is " + str( size )+ " bytes" )
                    create_time_stamp = os.path.getctime(file_abs_path)
                    #print("the created time of this file is "+ str(create_time_stamp)  )
                    base_name = os.path.basename(file_abs_path)
                    common_prefix = os.path.commonprefix(file_abs_path)
                    dir_name = os.path.dirname(file_abs_path)
                    exists = os.path.exists(file_abs_path)
                    lexists = os.path.lexists(file_abs_path)
                    expanduser = os.path.expanduser(file_abs_path)
                    expandvars = os.path.expandvars(file_abs_path)
                    get_a_time = os.path.getatime(file_abs_path)
                    get_m_time = os.path.getmtime(file_abs_path)
                    get_c_time = os.path.getctime(file_abs_path)
                    isabs = os.path.isabs(file_abs_path)
                    is_file = os.path.isfile(file_abs_path)
                    is_dir = os.path.isdir(file_abs_path)
                    is_link = os.path.islink(file_abs_path)
                    is_mount = os.path.ismount(file_abs_path)
                    norm_case = os.path.normcase(file_abs_path)
                    norm_path = os.path.normpath(file_abs_path)
                    real_path = os.path.realpath(file_abs_path)
                    
                   # same_file = os.path.samefile(file_abs_path)
                    #same_open_file = os.path.sameopenfile(file_abs_path)
                    #6:converts all properties and attributes to a dictionary
                    properties_and_attributes = {"size_of_file": size ,"create_time_stamp": create_time_stamp,"abs_path":file_abs_path,"base_name":base_name,"common_prefix": common_prefix,"dir_name":dir_name,"exists":exists,"lexists":lexists,"expanduser": expanduser,"expandvars":expandvars,"get_a_time":get_a_time,"get_m_time":get_m_time,"get_c_time":get_c_time,"isabs":isabs,"is_file":is_file,"is_dir":is_dir, "is_link":is_link,"is_mount": is_mount,"norm_case":norm_case,"norm_path":norm_path,"real_path":real_path,"run_id":run_id}
                    l =properties_and_attributes
                    j_dumps = json.dumps(l)#7 python writes to json

                    #8 python writes to new file
                    with open(output_file_name, 'a') as f:
                        
                            #if old_abs_path == False:
                        #epoch_time = datetime.datetime(max_c_time).timestamp()
                        if  datetime.strptime(max_c_time, '%Y-%m-%d %H:%M:%S.%f').timestamp() < get_c_time or datetime.strptime(max_m_time, '%Y-%m-%d %H:%M:%S.%f').timestamp() < get_m_time:
                            
                            json.dump(l, f)
                            f.write("\n")
                            print(file_abs_path+'new' + max_c_time + '-'+ str(datetime.fromtimestamp(get_c_time)))
                        #else:
                            #print(file_abs_path+'old')
                                
                except Exception as e:
                    properties_and_attributes = {"size_of_file": size ,"create_time_stamp": create_time_stamp,"abs_path":file_abs_path,"base_name":base_name,"common_prefix": common_prefix,"dir_name":dir_name,"exists":exists,"lexists":lexists,"expanduser": expanduser,"expandvars":expandvars,"get_a_time":get_a_time,"get_m_time":get_m_time,"get_c_time":get_c_time,"isabs":isabs,"is_file":is_file,"is_dir":is_dir, "is_link":is_link,"is_mount": is_mount,"norm_case":norm_case,"norm_path":norm_path,"real_path":real_path,"run_id":run_id}
                    l =properties_and_attributes
                    j_dumps = json.dumps(l)#7 python writes to json
                    print(e)
                        #8 python writes to new file
                    with open(output_file_name+"_error", 'a') as g:
                                json.dump(l, g)
                                #if old_abs_path == False:
                                g.write("\n")
                                
#file_inventory_creator('/home/ayan/stuff/','8th.json',max_a_time,max_m_time,max_c_time, run_id)
                      

            
            
            
import psycopg2
import pandas
pd = pandas
def json_inserter(file_name,table_name):
        pgconn = psycopg2.connect(
        host= 'localhost',
        user = 'dev_test1',
        password = 'ayan',
        database = 'testing')
        pgcursor = pgconn.cursor()
       
        df = pd.read_json (file_name,lines=True)
        from sqlalchemy import create_engine
        engine = create_engine('postgresql+psycopg2://dev_test1:ayan@localhost/testing')
        print(engine)
        df.to_sql(table_name, engine, if_exists = 'append', index=False)
        
        
#json_inserter('8th.json','file_inventory')

import psycopg2
import pandas
pd = pandas
def max_dates_func(run_id,source_system_id):    
    pgconn2 = psycopg2.connect(
        host= 'localhost',
        user = 'dev_test1',
        password = 'ayan',
        database = 'testing')
    pgcursor2 = pgconn2.cursor()
    print(pgcursor2.execute('INSERT INTO max_dates (create_a_time,create_m_time,create_c_time,run_id,source_system_id)'+ 
'SELECT MAX(get_a_time) AS "max_a_time",'+
'MAX(get_m_time) AS "max_m_time",' +
'MAX(get_c_time) AS "max_c_time",'+
run_id +  ",'" +
source_system_id + "'"
'FROM file_inventory'))
    pgconn2.commit()
    pgcursor2.close()
    pgconn2.close()
    
import platform
g = (platform.uname())
source_system_id = g[0]+" "+ g[1]
pgconn = psycopg2.connect(
    host= 'localhost',
    user = 'dev_test1',
    password = 'ayan',
    database = 'testing')
pgcursor = pgconn.cursor()
max_a_time= result [0][1]
max_m_time= result [0][2]
max_c_time= result [0][3]
run_id= result [0][4] + 1
#max_dates_func(str(run_id),source_system_id)




