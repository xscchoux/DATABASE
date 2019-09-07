from src import CSVDataTable
import csv
import json
import logging
import time
import sys

logging.basicConfig(level=logging.INFO)

def load(fn):

    result = []
    cols = None
    with open(fn, "r", encoding='utf_8_sig') as infile:
        rdr = csv.DictReader(infile)
        cols = rdr.fieldnames
        for r in rdr:
            result.append(r)

    return result, cols

def test_insert():
    sys.stdout = open("../test_output/insert.txt", "w")    #redirect console output to a TXT file
    print("test insert\n==============\n")
    new_r, cols = load("..\CSVFile\BattingSmall.csv")
    t = CSVDataTable.CSVDataTable(table_name="batting2", column_names=cols,
                                            primary_key_columns=['playerID'], loadit=None)
    r = {"playerID":"newID","yearID":"2222"}
    print("Insert a row")
    t.insert(r)
    print("t = ",t)
    print()
    print("Getting rows:",t.get_rows())

# test_insert()

def test_save():
    sys.stdout = open("../test_output/save.txt", "w")    #redirect console output to a TXT file
    print("test save\n==============\n")
    rows, cols = load("..\CSVFile\profile.csv")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=cols, primary_key_columns=['uni'], loadit=None)
    t.import_data(rows)
    print("Table = ",t)
    t.add_index("Name", ['last_name'], "INDEX")
    t.save()
    print("generate rings.json")

# test_save()
# this gives rings.json

def test_save2():
    sys.stdout = open("../test_output/save2.txt", "w")    #redirect console output to a TXT file
    print("test save(2)\n==============\n")
    rows, cols = load("..\CSVFile\BattingSmall.csv")
    t = CSVDataTable.CSVDataTable(table_name="BattingSmall", column_names=cols, primary_key_columns=['playerID','yearID','teamID'], loadit=None)
    t.import_data(rows)
    print("Table = ",t)
    print(cols)
    t.add_index("Name", ['playerID', 'yearID', 'teamID'], "PRIMARY")
    t.save()
# test_save2()
# this gives BattingSmall.json

def test_load():
    sys.stdout = open("../test_output/load.txt", "w")    #redirect console output to a TXT file
    print("test load\n==============\n")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=['uni','email','last_name','first_name'], primary_key_columns=['uni'], loadit=None)
    # print("Table = ",t)
    print('indexes loaded:\n')
    t.load()

# test_load()  # load from rings.json

def test_add_index():
    sys.stdout = open("../test_output/add_index.txt", "w")    #redirect console output to a TXT file
    print("test add_index\n==============\n")
    i = CSVDataTable.Index(name="Bob",table="rings2", columns=["last_name","first_name"], kind="PRIMARY")
    r = {"last_name":"Ferguson","first_name":"Donald","uni":"sure"}
    r2 = {"last_name": "Chou", "first_name": "SC", "uni": "sure"}
    kv = i.compute_index_value(r)

    print("KV = ", kv)
    i.add_to_index(row=r, rid="2")
    i.add_to_index(row=r2, rid="3")
    # print(i._index_data)
    print("I = ", i)
# test_add_index()

def test_delete():   # need modification
    sys.stdout = open("../test_output/delete.txt", "w")    #redirect console output to a TXT file
    print("test delete\n==============\n")
    rows, cols = load("..\CSVFile\profile.csv")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=cols, primary_key_columns=['uni'], loadit=None)
    t.import_data(rows)
    print("Table = ",t)
    print()
    tmp = {"last_name":'Gamgee'}
    print("template = ", tmp)
    num, deleted = t.delete(tmp)
    print("\n",num,"rows deleted:\n", deleted)
    print()
    print("The rows become:")
    print(t.get_rows_with_rids())

# test_delete()

def test_find_by_template():
    sys.stdout = open("../test_output/find_by_template.txt", "w")    #redirect console output to a TXT file
    print("test find_by_template\n==============\n")
    rows, cols = load("../CSVFile/people.csv")
    t = CSVDataTable.CSVDataTable(table_name="people", column_names=cols, primary_key_columns=['playerID'])
    t.import_data(rows)
    print("T =", t)
    tmp = {"playerID": "willite01"}

    print("\nWithout using indexes")
    start = time.time()
    for i in range(0,1000):
        r = t.find_by_template(tmp, fields=cols, use_index=False)
        if i == 0:
            print('Row = ', r)
    end = time.time()
    elapsed = end - start
    print("Elapsed time = ", elapsed,"\n")

    print("\nUsing indexes")
    t.add_index("Name", cols, "INDEX")  #loading index data
    start = time.time()
    for i in range(0,1000):
        r = t.find_by_template(tmp, fields=cols, use_index=True)  # Index._index_data is gone!
        if i == 0:
                print("Row = ", r)
    end = time.time()
    elapsed = end - start
    print("Elapsed time = ", elapsed)

# test_find_by_template()

def join_and_query():
    sys.stdout = open("../test_output/join_and_query_optimization.txt", "w")    #redirect console output to a TXT file
    print("test join_and_query_optimization\n==============\n")

    rows, cols = load("../CSVFile/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People", column_names=cols, primary_key_columns=['playerID'])
    t.import_data(rows)
    print("T = ", t)

    rows, cols = load("../CSVFile/Batting.csv")
    t2 = CSVDataTable.CSVDataTable(table_name="Batting", column_names=cols, primary_key_columns=['playerID', 'teamID', 'yearID', 'stint'])
    t2.import_data(rows)
    print("T = ", t2)

    start = time.time()
    j = t2.join(t, ['playerID'],
                w_clause={"People.nameLast":"Williams", "People.birthCity":"San Diego"},
                p_clause=["playerID", "People.nameLast", "People.nameFirst", "Batting.teamID",
                        "Batting.yearID","Batting.stint","Batting.H","Batting.AB"], optimize=True)
    # print("Result =", j)
    print("All rows = ", json.dumps(j.get_rows(), indent=2))
    end = time.time()
    elapsed = end - start
    print("Elapsed time (with optimization) = ", elapsed)

    print()

    start = time.time()
    j = t2.join(t, ['playerID'],
                w_clause={"People.nameLast":"Williams", "People.birthCity":"San Diego"},
                p_clause=["playerID", "People.nameLast", "People.nameFirst", "Batting.teamID",
                        "Batting.yearID","Batting.stint","Batting.H","Batting.AB"], optimize=False)
    # print("Result =", j)
    print("All rows = ", json.dumps(j.get_rows(), indent=2))
    end = time.time()
    elapsed = end - start
    print("Elapsed time (without optimization) = ", elapsed)

join_and_query()