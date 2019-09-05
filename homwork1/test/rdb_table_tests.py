import sys
import csv
sys.path.append("C:/Users/User/Desktop/Introduction to databases/HW1/src")
from RDBDataTable import RDBDataTable
import json

file = open("rdb_table_test.txt","w")

# find by primary key
print("find by primary key")
s = RDBDataTable('People', ["playerID"], None, False)
result = s.find_by_primary_key( ["'willite01'"],field_list=['playerID', 'nameLast'])
print(result)

file.write("\n\nTest find by primary key")
file.write('\nTable:People.csv, key = ["willite01"], field_list=["playerID", "nameLast"]\n')
file.write("Result:"+str(result) + '\n')

# # find by template
print("find by template")
tmp = {"nameLast": "Williams", "throws": "R"}
result = s.find_by_template(tmp, field_list=['playerID', 'nameLast', 'birthCity', 'throws'])
print(result)

file.write("\n\nTest find by template")
file.write('\nTable:People.csv, tmp = {"nameLast": "Williams", "throws": "R"}\n')
file.write("Result:"+ str(result) + '\n')


s2 = RDBDataTable("offices", ["officeCode"], None, False)
# insert
tmp = {"city": "Tokyo"}
result1 = s2.find_by_template(tmp, field_list = ['officeCode','city','phone','addressLine1','addressLine2','state'])

new_r = {'officeCode': '201', 'city': 'Tokyo'}
file.write("\n\nTest insert")
file.write('\nTable:offices.csv, key = "officeCode": "1", new_r = {"officeCode": "201", "city": "Tokyo"}\n')
file.write("\nBefore insert:" + str(result1) + '\n')
s2.insert(new_r)
result2 = s2.find_by_template(tmp,field_list = ['officeCode','city','phone','addressLine1','addressLine2','state'])
print("\n\nAfter insert,  ", result2)
file.write("After insert:" + str(result2) + '\n')

# update by key
key = ['1']
new_v = {'state':'"Mars"','country':'"Jupiter"'}
result = s2.find_by_primary_key(key, field_list=['officeCode','city','phone','addressLine1','addressLine2','state','country'])
file.write("\n\nUpdate by template")
file.write('\nTable:Offices.csv,  key:  OfficeCode = 1 , new_v = {"state": "Mars", "country": "Jupiter"}\n')
file.write("\nBefore update," + str(result) + "\n")
result = s2.update_by_key(key, new_v)
print("\n\nBefore update,  ", str(result))
result2 = s2.find_by_primary_key(key,field_list=['officeCode','city','phone','addressLine1','addressLine2','state','country'])
print("\n\nAfter update,  ", str(result2))
file.write("\nAfter update," + str(result2) + "\n")

# update by template
tmp = {'city': 'Boston'}
new_v = {'state': '"Mars"', 'country': '"Jupiter"'}
pre_result = s2.find_by_template(tmp,field_list=['officeCode','city','phone','addressLine1','addressLine2','state','country'])
print('Before update... ', pre_result)
file.write("\n\nUpdate by template")
file.write('\nTable:Offices.csv,  tmp = {"city":"Boston"} , new_v = {"state":"Mars","country":"Jupiter"}\n')
file.write("\nBefore update," + str(pre_result) + "\n")
s2.update_by_template(tmp, new_v)
result = s2.find_by_template(tmp,field_list=['officeCode','city','phone','addressLine1','addressLine2','state','country'])
print('\n\nAfter update ...', result)
file.write("\nAfter update," + str(result) + '\n')

# delete by template
tmp = {'city': 'Paris'}
result1 = s2.find_by_template(tmp,field_list = ['officeCode','city','phone','addressLine1','addressLine2','state'])
file.write("\n\nTest delete by template")
file.write('\nTable:Offices.csv, tmp = {"city":"Paris"}\n')
print("\n\nBefore delete, ", result1)
file.write("\nBefore delete," + str(result1) + '\n')
s2.delete_by_template(tmp)

result2 = s2.find_by_template(tmp,field_list = ['officeCode','city','phone','addressLine1','addressLine2','state'])
print("\n\nAfter delete,  ", result2)
file.write("\nAfter delete," + str(result2) + '\n')

# delete by key

file.write("\n\nTest delete by key")
file.write('\nTable:Offices.csv, key:  "officeCode" = "1"\n')
pre_result = s2.find_by_primary_key(['1'],field_list = ['officeCode','city','phone','addressLine1','addressLine2','state'])
file.write("\nBefore delete," + str(pre_result) + "\n")
result = s2.delete_by_key(['"1"'])
result2 = s2.find_by_primary_key(['1'],field_list = ['officeCode','city','phone','addressLine1','addressLine2','state'])
print("\nAfter delete," + str(result2) +"\n")
file.write("\nAfter delete," + str(result2) + '\n')
