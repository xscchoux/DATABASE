import sys
import csv
sys.path.append("C:/Users/User/Desktop/Introduction to databases/HW1/src")
from CSVDataTable import CSVDataTable

# from DerivedDataTable import DerivedDataTable
import json

file = open("csv_table_test.txt","a")


def test_create_it():
    tbl = CSVDataTable("People",
                       {
                           "directory":"../Data",
                           "filename":"People.csv"
                       },
                       ['playerID'],None)
    tbl.load()
    print("First Table = ",tbl)

def test_match_by_template():
    tbl = CSVDataTable("People",
                       {
                           "directory":"../Data",
                           "filename":"People.csv"
                       },
                       ['playerID'],None)
    tbl.load()
    print("First Table = ",tbl)
    tmp = {"nameLast": "Aardsma","nameFirst":"David"}
    result = tbl.matches_template(tmp,tbl._rows[0])
    file.write(tbl.__str__()+ '\n')
    file.write('Test match by template')
    print("Match = ",result)
    file.write('Table:People.csv, tmp = {"nameLast": "Aardsma","nameFirst":"David"}')


def test_find_by_tmp():
    tbl = CSVDataTable("People",
                       {
                           "directory":"../Data",
                           "filename":"People.csv"
                       },
                       ['playerID'],None)
    tbl.load()
    print("First Table = ",tbl)
    tmp = {"nameLast": "Williams","throws":"R"}
    file.write("\n\nTest find by template")
    file.write('\nTable:People.csv, tmp = {"nameLast": "Williams","throws":"R"} , field_list=["playerID", "nameLast", "birthCity","throws"]\n')
    result = tbl.find_by_template(tmp,field_list=['playerID', 'nameLast', 'birthCity','throws'])
    print("\n\n After find by template, result = ", result)

    file.write("Result:"+str(result) + '\n')

    tmp2 = {'birthCity':'San Diego'}
    result2 = result.find_by_template(tmp2)
    print("\n\n After find by template, find by template2, result = ", result2)

def test_key():
    tbl = CSVDataTable("People",
                       {
                           "directory":"../Data",
                           "filename":"People.csv"
                       },
                       ['playerID'],None)
    tbl.load()
    print("First Table = ",tbl)
    result = tbl.find_by_primary_key(['willite01'], field_list=['playerID', 'nameLast'])
    print("\nResult = ", result)
    file.write("\n\nTest find by primary key")
    file.write('\nTable:People.csv, key = ["willite01"], field_list=["playerID", "nameLast"]\n')
    file.write("Result:"+str(result) + '\n')

def test_insert():
    tbl = CSVDataTable("Offices",
                       {
                           "directory":"../Data",
                           "filename":"Offices.csv"
                       },
                       ['officeCode'],None)
    tbl.load()
    print("First Table = ",tbl)

    tmp = {'city':'Tokyo'}
    result1 = tbl.find_by_template(tmp)
    print("\n\nBefore insert, ", result1)

    new_r = {'officeCode':'201','city':'Tokyo'}
    tbl.insert(new_r)

    result2 = tbl.find_by_template(tmp)
    print("\n\nAfter insert,  ",result2)
    file.write("\n\nTest insert")
    file.write('\nTable:Offices.csv, tmp = {"city":"Tokyo"}\n')
    file.write("Before insert,"+str(result1) + '\n')
    file.write("\nAfter insert,"+str(result2) + '\n')

def test_delete_by_template():
    tbl = CSVDataTable("Offices",
                       {
                           "directory":"../Data",
                           "filename":"Offices.csv"
                       },
                       ['officeCode'],None)
    tbl.load()
    print("First Table = ",tbl)

    tmp = {'city':'Paris'}
    result1 = tbl.find_by_template(tmp)
    print("\n\nBefore delete, ", result1)

    result = tbl.delete_by_template(tmp)
    print("\n\nI deleted ...", result, "rows")

    result2 = tbl.find_by_template(tmp)
    print("\n\nAfter delete,  ",result2)
    file.write("\n\nTest delete by template")
    file.write('\nTable:Offices.csv, tmp = {"city":"Paris"}\n')
    file.write("\nBefore delete,"+ str(result1) + '\n')
    file.write("\nAfter delete,"+str(result2) + '\n')

def test_delete_by_key():
    tbl = CSVDataTable("Offices",
                       {
                           "directory":"../Data",
                           "filename":"Offices.csv"
                       },
                       ['officeCode'],None)
    tbl.load()
    print("First Table = ",tbl)
    file.write("\n\nTest delete by key")
    pre_result = tbl.find_by_primary_key(['1'])
    file.write("\nBefore delete," +  str(pre_result) + "\n")
    result = tbl.delete_by_key(['1'])


    print("\nI deleted ...", result," rows")
    result2 = tbl.find_by_primary_key(['1'])
    print("\n\nAfter delete,  ",tbl)
    file.write('\nTable:Offices.csv, key:  "officeCode" = "1"\n')
    file.write("\nAfter delete," + str(result2) + '\n')

def test_update_by_template():
    tbl = CSVDataTable("Offices",
                       {
                           "directory":"../Data",
                           "filename":"Offices.csv"
                       },
                       ['officeCode'],None)
    tbl.load()
    tmp = {'city':'Boston'}
    new_v = {'state':'Mars','country':'Jupiter'}
    # print("Row = ", tbl._rows[0])
    # new_row = tbl._update_row(tbl._rows[0],new_v)
    # print("New rows = ", new_row)
    pre_result = tbl.find_by_template(tmp)
    print('Before update... ', pre_result)
    file.write("\n\nUpdate by template")
    file.write('\nTable:Offices.csv,  tmp = {"city":"Boston"} , new_v = {"state":"Mars","country":"Jupiter"}\n')
    tbl.update_by_template(tmp, new_v)
    file.write("\nBefore update," +  str(pre_result) + "\n")
    result = tbl.find_by_template(tmp)
    print('\n\nAfter update ...', result)
    file.write("\nAfter update," + str(result) + '\n')


def test_update_by_key():
    tbl = CSVDataTable("Offices",
                       {
                           "directory":"../Data",
                           "filename":"Offices.csv"
                       },
                       ['officeCode'],None)
    tbl.load()
    key = ['1']
    new_v = {'state': 'Mars', 'country': 'Jupiter'}
    result = tbl.find_by_primary_key(key)
    file.write("\n\nUpdate by template")
    file.write('\nTable:Offices.csv,  key:  OfficeCode = 1 , new_v = {"state": "Mars", "country": "Jupiter"}\n')
    file.write("\nBefore update," + str(dict(result)) + "\n")
    print("\n\nBefore update,  ",dict(result))
    result = tbl.update_by_key(key, new_v)
    result2 = tbl.find_by_primary_key(key)
    print("\n\nAfter update,  ",dict(result2))
    file.write("\nAfter update," + str(dict(result2)) + "\n")

# test_create_it()
# test_match_by_template()
test_find_by_tmp()
test_key()
test_insert()
test_delete_by_template()
test_delete_by_key()
test_update_by_template()
test_update_by_key()
