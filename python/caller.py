
import psycopg2
import platform
from first import file_inventory_creator
from first import json_inserter
from first import max_dates_func
import psycopg2
import pandas
pd = pandas

g = (platform.uname())
source_system_id = g[0]+" "+ g[1]
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
print(max_c_time)

file_inventory_creator('/home/ayan/stuff/','9th.json',max_a_time,max_m_time,max_c_time, run_id)

json_inserter('9th.json','file_inventory')

max_dates_func(str(run_id),source_system_id)
