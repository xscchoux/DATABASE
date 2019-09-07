#Shao-Chi Chou - sc4400
import pymysql.cursors
import json
import aeneid.utils.utils as ut
import aeneid.utils.dffutils as db
import aeneid.dbservices.DataExceptions
from aeneid.dbservices.RDBDataTable import RDBDataTable

db_schema = None                                # Schema containing accessed data
cnx = None                                      # DB connection to use for accessing the data.
key_delimiter = '_'                             # This should probably be a config option.

# Is a dictionary of {table_name : [primary_key_field_1, primary_key_field_2, ...]
# Used to convert a list of column values into a template of the form { col: value }
primary_keys = {}

# This dictionary contains columns mappings for nevigating from a source table to a destination table.
# The keys is of the form sourcename_destinationname. The entry is a list of the form
# [[sourcecolumn1, destinationcolumn1], ...
join_columns = {}

# Data structure contains RI constraints. The format is a dictionary with an entry for each schema.
# Within the schema entry, there is a dictionary containing the constraint name, source and target tables
# and key mappings.
ri_constraints = None

data_tables = {}


# TODO This is a bit of a hack and we should clean up.
# We should load information from database or configuration file.
people = RDBDataTable("lahman2017.people", key_columns=['playerID'])
data_tables["lahman2017.people"] = people
batting = RDBDataTable("lahman2017.batting", key_columns=['playerID', 'yearID', 'teamID', 'stint'])
data_tables["lahman2017.batting"] = batting
appearances = RDBDataTable("lahman2017.appearances", key_columns=['playerID', 'yearID', 'teamID'])
data_tables["lahman2017.appearances"] = appearances
offices = RDBDataTable("classiccars.offices", key_columns=['officeCode'])
data_tables["classiccars.offices"] = offices


def get_data_table(table_name):

    result = data_tables.get(table_name, None)
    if result is None:
        result = RDBDataTable(table_name)
        data_tables[table_name] = result
    print("result in get_data_table",result)
    return result

def get_by_template(table_name, template, field_list=None, limit=None, offset=None, order_by=None, commit=True):

    dt = get_data_table(table_name)
    result = dt.find_by_template(template, field_list, limit, offset, order_by, commit)
    result = result.get_rows()
    # related = dt.get_related_resource_names()
    # for r in result:
    #     r[related_resources] = related
    return result

def get_by_primary_key(table_name, key_fields, field_list=None, commit=True):

    dt = get_data_table(table_name)
    result = dt.find_by_primary_key(key_fields, field_list)
    # if result is not None:
    #     related = dt.get_related_resource_names()
    #     result['related_resources'] = related
    return result

def get_by_primary_key_path(table_name,query_param,key, sub_resource,
                            field_list=None, limit=None, offset=None, order_by=None,
                            commit=True):
    dt = get_data_table(table_name)
    print("get_by_primary_key_path")
    print("key=",key,"sub_resource=",sub_resource,"field_list=",field_list)
    result = dt.find_by_path_key(table_name, query_param, key, sub_resource, field_list, limit, offset, order_by)
    result = result.get_rows()
    print("result in get by primary key path",result)
    return result

def create(table_name, new_row, commit=True):
    result = None

    try:
        dt = get_data_table(table_name)
        result = dt.insert(new_row)
    except Exception as e:
        print(e)
        raise e
    return result

def get_primary_key_string(dbname, resource, key_or_tuple):
    pass
def get_related(table_name, row):
    pass

def update_by_key(table_name, key_values, new_row,commit = True):
    dt = get_data_table(table_name)
    result = dt.update_by_key(key_values, new_row)
    return result

def delete(table_name, key_cols):
    dt = get_data_table(table_name)
    result = dt.delete_by_key(key_cols)
    return result

def insert_by_path(table_name, key, related_name, new_r):   # returns the primary key
    # lahman2017.people ['willite01'] batting {'H': '100', 'HR': '50', 'AB': '100', 'yearID': '2', 'teamID': 'BOS', 'stint': '1'}

    try:
        dt = get_data_table(table_name)
        print('new_r',new_r,"related_name",related_name)
        result = dt.insert_related(key, new_r, related_name)  # ['willite01'] ,{'H': '100', 'HR': '50', 'AB': '100', 'yearID': '2', 'teamID': 'BOS', 'stint': '1'} ,batting
        print("after insert related, result = ", result)
    except Exception as e:
        print("insert by path error!")
        raise e

    return result

def get_by_query_from_h(table_name, child, resources, template, field_list):
    try:
        dt = get_data_table(table_name)
        result = dt.find_by_path_template(table_name,
                                          child_resources=child_resources,
                                          template=template,
                                          field_list=field_list,
                                          limit=None,
                                          offset=None,
                                          order_by=None)
    except Exception as e:
        print("get by query from h wrong!")
        raise e

    return result




















