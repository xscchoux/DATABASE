import csv
import json
import pandas
import copy
import logging

class Index():
    def __init__(self, name=None, table=None, columns=None, kind=None, loadit=None):
        """
        Implements a hash index using a Python dictionary
        :param name: Logical name of the index
        :param table: Reference to the table for which this is an index
        :param columns: The row columns that comprise the key, in the order they form the key.
        :param kind: One of "PRIMARY", "UNIQUE", "INDEX". Primary and Unique are synonyms and support one entry
        per key. INDEX supports multiple entries with the same key.
        :param loadit: If evaluates to True, the index is from the passed value. This is restoring an index from
        a file.
        """
        if loadit:
            logging.debug("Loading an index.")
            self.table_name = table
            self._index_data = None
            self.name, self.columns, self.kind, self.table = self.from_json(table, loadit)
            logging.debug("Loaded index. name=%s, column=%s, kind=%s, table=%s",
                          self.name, str(self.columns), self.kind, self.table)
        else:
            logging.debug("Creating index. name=%s, columns=%s, kind=%s, table=%s",
                          name, str(columns), kind, table)
            # columns.sort()
            self.name = name
            self.columns = columns
            self.kind = kind
            self.table = table
            self.table_name = table
            self._index_data = None

        # if self.columns is not None:
        #     self.columns.sort()

    def compute_index_value(self, row):
        # print("row",row)
        # print(self.columns)
        key_v = [row[k] for k in self.columns]
        key_v = "_".join(key_v)
        return key_v
        # print("compute",self.columns)
        # return row[self.columns[0]]

    def add_to_index(self,row,rid):
        """

        :param row: The row to add to (reference from) the index.
        :param rid: The row ID of the row in the table.
        :return: None
        """
        index_key = self.compute_index_value(row)
        # print(index_key)

        if self._index_data is None:
            self._index_data = {}

        # Which index bucket/entry will hold the reference,
        bucket = self._index_data.get(index_key, None)

        # If the bucket exists, there is an entry. If the index type is not INDEX,
        # this is a duplicate error.
        if self.kind != "INDEX" and bucket is not None:
            raise KeyError("Duplicate key: index = " + self.name + ", table=" + self.table_name + \
                           ", key = " + index_key)
        else:
            if bucket is None:
                bucket = {}

            # We treat a bucket as a dictionary. This simplifies deleting a row from an index.
            bucket[rid] = row
            self._index_data[index_key] = bucket

    # def remove_from_index(self, row, rid):
    #     pass
    def _build(self):
        pass
    def __str__(self):
        result = ""
        result += "Index name: " + self.name
        result += "\nKind: " + self.kind
        result += "\nIndex columns: " + str(self.columns)
        result += "\nTable name: " + str(self.table_name)

        if self._index_data is not None:
            keys = list(self._index_data.keys())
            result += "\nNo. of unique index values: " + str(len(keys))
            cnt = min(5, len(keys))
            result += "Entries:\n"
            for i in range(0, cnt):
                result += "[" + keys[i] + ":" + json.dumps(self._index_data[keys[i]], indent=2) + "]\n"

        return result
    def to_json(self):
        """
            Convert the index data and state to a JSON object.
            :return: JSON representation
            """
        result = {}
        result["name"] = self.name
        result["columns"] = self.columns
        result["kind"] = self.kind
        result["table_name"] = self.table_name
        result["index_data"] = self._index_data
        return result

    def from_json(self, table, loadit):
        name = loadit['name']
        columns = loadit['columns']
        kind = loadit['kind']
        table = loadit['table_name']
        return name, columns, kind, table

    def matches_index(self,template):
        """
        Determines if this index supports/matches the query template. Since the indexes are has indexes,
        the index matches if the index columns are a subset of the template columns
        :param template:  The query template
        :return:
                -- None if the index does not match.
                -- The number of distinct entries in the index if it does match. This allows the query engine
                to determine the index selectivity.
        """
        k = set(list(template.keys()))
        c = set(self.columns)

        if c.issubset(k):
            # Index matches. Return the number of distinct index entries.
            if self._index_data is not None:
                kk = len(self._index_data.keys())  # how many keys i the index
            else:
                kk = 0
        else:
            kk = None

        return kk

    def find_rows(self,tmp):
        """
        Using the index, find the matching rows. That is, the rows with a key value that matches the template.
        :param tmp: Query template
        :return: Row IDs that match template.
        """
        # Get the values of the key columns, in the index column order.
        t_keys = tmp.keys()
        t_vals = [tmp[k] for k in self.columns]
        # Convert to an index key string
        t_s = "_".join(t_vals)

        # Get the corresponding index bucket.
        d = self._index_data.get(t_s, None)
        # Return a list of the keys (which are RIDs) in the bucket.
        if d is not None:
            d = list(d.keys())
        return d

    def get_no_of_entries(self):
        return len(list(self._index_data.keys()))

class CSVDataTable():
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. will extend the
    base class and implement the abstract methods.
    """
    _default_directory = "../DB/"
    # Default directory for CSV files

    def __init__(self, table_name, column_names=None, primary_key_columns=None, loadit=False):
        """

        :param table_name: Logical name of the table. Also name of file in DB directory.
        :param column_names:  Name of allowed columns in the data table.
        :param primary_key_columns: List, in order, of the columns (fields) that comprise the primary key.
        :param loadit: If this is true, the load method will set the values
        """
        self._table_name = table_name
        self._primary_key_columns = primary_key_columns
        self._column_names = column_names
        self._rows = None

        # Dictionary containing index data structures. (a dict of index name and index obj)
        self._indexes = None

        if not loadit:
            # Some parameters are mandatory if this is not a load.
            if column_names is None or table_name is None:
                raise ValueError("Did not provide table_name for column_names for table create.")

            self._next_row_id =1

            # Dictionary that will hold the rows
            self._rows = {}

            if primary_key_columns:
                self.add_index("PRIMARY", self._primary_key_columns, "PRIMARY")

    def get_table_name(self):
        return self._table_name

    def add_index(self, index_name, column_list, kind):
        if self._indexes is None:
            self._indexes = {}
        # I should check to make sure this is not duplicate index name
        self._indexes[index_name] = Index(name=index_name,table=self._table_name, columns=column_list, kind = kind)
        self.build(index_name)

    def build(self, i_name):

        idx = self._indexes[i_name]
        # print(idx)
        for k,v in self._rows.items():
            idx.add_to_index(v, k)
        # print(idx)
    def drop_index(self, index_name):
        pass
    def __str__(self):

        result = str(type(self)) + ": name = " + self._table_name
        result += "\nprimary_key_columns = " + str(self._primary_key_columns)

        if self._column_names is not None:
            result += "\ncolumn names = " + str(self._column_names)
        if self._rows is not None:
            row_count = len(self._rows)
        else:
            row_count = 0
        result += "\nNo. of rows = " + str(row_count)

        to_print = min(5,row_count)
        count = 0
        for i in self._rows.items():
            if count > to_print:
                break
            result +=  "\n" + str(i)
            count += 1
        return result

    def _get_primary_key(self,r):
        pass
    def _get_primary_key_string(self,r):
        pass
    def _get_next_row_id(self):
        self._next_row_id += 1
        return self._next_row_id

    def _add_row(self,r):
        """

            :param r:  A 'row' from the data table. The type is a type compatible with dict
            :return: None
            """
        if self._rows is None:
            self._rows = {}

        rid = self._get_next_row_id()
        self._rows[rid] = r

        if self._indexes is not None:
            for n, idx in self._indexes_items():
                idx.add_to_index(r, rid)

    def _remove_row(self,rid):
        """
            :param rid:  A 'row' from the data table. The type is a type compatible with dict
            :return: None
            """
        for n, idx in self._indexes.items():
            idx.remove_from_index(rid)
        del[self._indexes[rid]]

    def import_data(self, import_data):
        for r in rows:
            self.insert(r)

    def save(self):
        d = {
            "state":{
                "table_name": self._table_name,
                "primary_key_columns": self._primary_key_columns,
                "next_rid":self._get_next_row_id(),
                "column_names": self._column_names
            }
        }
        fn = CSVDataTable._default_directory + self._table_name +".json"
        d["rows"] = self._rows
        # print(json.dumps(self._rows, indent=2))

        for k,v in self._indexes.items():
            idxs = d.get("indexes", {})
            idx_string = v.to_json()
            idxs[k] = idx_string
            d['indexes'] = idxs

        d = json.dumps(d, indent=2)
        print(d)
        with open(fn, "w+") as outfile:
            outfile.write(d)

    def load(self):
        fn = CSVDataTable._default_directory + self._table_name +".json"
        with open(fn,"r") as infile:
            d = json.load(infile)

            state = d['state']
            self._table_name = state["table_name"]
            self._primary_key_columns = state["primary_key_columns"]
            self._next_row_id = state["next_rid"]
            self.column_names = state["column_names"]
            self._rows = d['rows']

            for k,v in d["indexes"].items():
                idx = Index(loadit=v, table=self._table_name)
                if self._indexes is None:
                    self.indexes = {}
                self._indexes[k] = idx
                idx._index_data = v

            # for k,v in d["indexes"].items():
            #     idx = Index
            for k,v in self._indexes.items():
                print(k,v.to_json())
                print()


    def get_rows_with_rids(self):
        return self._rows
    def get_rows(self):
        list01 = []
        for xx in self._rows.values():
            list01.append(xx)
        return list01

    def matches_template(self,row,tmp):
        if tmp is None:
            return True
        keys = tmp.keys()
        for k in keys:
            v = row.get(k,None)
            if tmp[k] != v:
                return False
        return True

    def get_best_index(self, t):
        """
        :param t: Template we are using
        :return: Most selective index
        """
        best = None
        n = None

        # If I have indexes.
        if self._indexes is not None:

            # For every index name and index object
            for k,v in self._indexes.items():
                # print(v)
                # Determine if this index matches the template
                cnt = v.matches_index(t)

                # If it does
                if cnt is not None:
                    # If I don't have a best. This is the best.
                    if best is None:
                        best = cnt      # Best value is cnt
                        n = k           # Name of best index is n
                    else:
                        if cnt > best:  # This one is "better"
                            best = v.get_no_of_entries()    #   Count the keys, number of distinct index entries
                            n = k
        return n

    def get_index_and_selectivity(self, cols):
        on_template = dict(zip(cols, [None]*len(cols)))   # playerID: None
        best = None
        n = self.get_best_index(on_template)

        if n is not None:
            best = len(list(self._rows.keys()))/(self._indexes[n].get_no_of_entries())

        return n, best

    def find_by_index(self,tmp,idx):
        # print("idx=",idx._index_data)   # _index_data is gone here
        r = idx.find_rows(tmp)
        res = [self._rows[k] for k in r]
        return res

    def find_by_scan_template(self,tmp,rows,fields=None):
        """
        :param tmp: A dictionary of the form { "field1" : value1, "field2": value2, ...}. The function will return
            a derived table containing the rows that match the template.
        :param rows: All rows in the table
       :param fields: fields
        """
        if rows is None or tmp is None:
            print("error in find by scan template")
            return None
        some_rows = []
        for i in rows:
            if self.matches_template(i,tmp):
                some_rows.append(i)
        new_rows = []
        for xx in some_rows:
            temp = {}
            for yy in fields:
                temp[yy] = xx[yy]
            new_rows.append(temp)
        return new_rows

    def find_by_template(self, tmp, fields=None, use_index=True):
        """
        :param tmp: Template to match
        :param fields: Fields to get, i.e. project clause
        :param use_index:If True, can use index if false cannot
        :return:
        """
        idx = self.get_best_index(tmp)    # idx = PRIMARY
        logging.debug("Using index = %s",idx)
        if idx is None or use_index == False:
            result = self.find_by_scan_template(tmp, self.get_rows(), fields)
        else:
            idx = self._indexes[idx]    #  idx:Index object    print: Index name: PRIMARY Kind: PRIMARY Index columns: ['playerID'] Table name: people
            # print(self._indexes)
            # print("find by temp",idx._index_data)
            res = self.find_by_index(tmp,idx)
            result = self.find_by_scan_template(tmp, res, fields)

        new_t = CSVDataTable(table_name="Derived:" + self._table_name, loadit = True)
        new_t.load_from_rows(table_name="Derived:" + self._table_name, rows = result)
        return new_t
        # return result

    def insert(self,r):
        if self._rows is None:
            self._rows = {}
        rid = self._get_next_row_id()
        self._rows[rid] = copy.copy(r)

        for k,v in self._indexes.items():
            v.add_to_index(r, rid)

        self._rows[rid] = copy.copy(r)

    def delete(self,tmp):
        count = 0
        somerows = {}
        rid = []
        key = tmp.keys()
        for item in self._rows.items():
            for k in key:
                if tmp[k] == item[1][k]:
                    count +=1
                    somerows.update({item[0]:item[1]})
                    rid.append(item[0])
        for xx in rid:
            del self._rows[xx]

        return count, somerows

    def import_data(self,rows):
        for r in rows:
            self.insert(r)

    def _get_sub_template(self, temp, table_name):
        pass

    def load_from_rows(self,table_name,rows):
        self._table_name = table_name
        self._column_names = None
        self._indexes = None
        self._rows = {}
        self._next_row_id = 1

        # print(rows)
        for r in rows:
            if self._column_names is None:
                self._column_names = list(sorted(r.keys()))
            self._add_row(r)


    @staticmethod
    def _get_scan_probe(l_table, r_table, on_clause):
        # s_best:the best index on the left table
        s_best, s_selective = l_table.get_index_and_selectivity(on_clause)
        r_best, r_selective = r_table.get_index_and_selectivity(on_clause)

        result = l_table, r_table

        if s_best is None and r_best is None:
            result = l_table, r_table
        elif s_best is None and r_best is not None:
            result = r_table, l_table
        elif s_best is None and r_best is None:
            result = l_table, r_table
        elif s_best is not None and r_best is not None and s_selective < r_selective:
            result = r_table, l_table

        return result

    def _get_specific_where(self,wc):

        result = {}
        if wc is not None:
            for k,v in wc.items():
                kk = k.split(".")
                if len(kk) == 1:      # if there is not dot
                    result[k] = v
                elif kk[0] == self._table_name:
                    result[kk[1]] = v
        if result == {}:
            result = None

        return result

    def _get_specific_project(self, p_clause):

        result = []
        if p_clause is not None:
            for k in p_clause:
                kk = k.split(".")
                if len(kk) == 1:
                    result.append(k)
                elif kk[0] == self._table_name:
                    result.append(kk[1])

        if result == []:
            result = None

        return result

    @staticmethod
    def on_clause_to_where(on_c,r):

        result = {c:r[c] for c in on_c}
        return result

    def join(self, r_table, on_clause, w_clause, p_clause, optimize=True):

        if optimize:
            s_table, p_table = self._get_scan_probe(self, r_table, on_clause)
        else:
            s_table = self        # Batting
            p_table = r_table     # People

        if s_table != self and optimize:
            logging.debug("Swapping tables.")
        else:
            logging.debug("Not swapping tables.")

        logging.debug("Before pushdown, scan rows = %s", len(s_table.get_rows()))

        if optimize:
            s_tmp = s_table._get_specific_where(w_clause)     # {'nameLast': 'Williams', 'birthCity': 'San Diego'}
            s_proj = s_table._get_specific_project(p_clause)   # ['playerID', 'nameLast', 'nameFirst']

            s_rows = s_table.find_by_template(s_tmp, s_proj)
            logging.debug("After pushdown, scan rows = %s", len(s_rows.get_rows()))
        else:
            s_proj = s_table._get_specific_project(p_clause)     # ['playerID', 'teamID', 'yearID', 'stint', 'H', 'AB']
            s_rows = []
            for kk in s_table.get_rows():
                temp = {}
                for rk, rv in kk.items():
                    if rk in s_proj:
                        temp[rk] = rv
                s_rows.append(temp)
            table = CSVDataTable(s_table._table_name, loadit = True)
            table.load_from_rows(s_table._table_name, s_rows)
            s_rows = table     # all rows in Batting (Orderdict(id:row))

        scan_rows = s_rows.get_rows()  #[OrderedDict([('playerID', 'willite01'),('birthCity', 'San Diego'), ('nameLast', 'Williams')]), OrderedDict([('playerID', 'willitr01'), ('birthCity', 'San Diego'), ('nameLast', 'Williams')])]

        result = []

        for r in scan_rows:
            p_where = CSVDataTable.on_clause_to_where(on_clause, r)  #{playerID:willite01}
            p_project = p_table._get_specific_project(p_clause)    # ['playerID', 'teamID', 'yearID', 'stint', 'H', 'AB']

            if optimize:
                p_rows = p_table.find_by_template(p_where, p_project)
            else:
                p_rows = p_table.find_by_template(p_where, p_table._column_names)
            p_rows = p_rows.get_rows()    # a list of all OrderedDict

            if p_rows:
                for r2 in p_rows:
                    new_r = {**r, **r2}
                    result.append(new_r)  # a list of normal Dict

        if optimize is not True:
            dict01 = {}
            if w_clause is not None:
                for k, v in w_clause.items():
                    kk = k.split(".")
                    if len(kk) == 1:  # if there is not dot
                        dict01[k] = v
                    elif kk[0] == self._table_name or kk[0] == r_table._table_name:
                        dict01[kk[1]] = v
            result2 =[]
            for xxx in result:
                if set(xxx.items()) >= set(dict01.items()):
                    result2.append(xxx)
            result = result2

        tn = "Join(" + self.get_table_name() + "," + r_table.get_table_name() + ")"  # tn = "Join(Batting, People)"
        final_result = CSVDataTable(table_name=tn, loadit=True)

        final_result.load_from_rows(table_name=tn, rows=result)

        ## Apply the template to the result table.

        return final_result


