from BaseDataTable import BaseDataTable
import csv
import copy

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. will extend the
    base class and implement the abstract methods.
    """
    def __init__(self, table_name, connect_info, key_columns=None, debug=True):
        """
        :param table_name: Name of the table. This is the table name for an RDB table or the file name for
            a CSV file holding data.
        :param connect_info: Dictionary of parameters necessary to connect to the data. See examples in subclasses.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
            A primary key is a set of columns whose values are unique and uniquely identify a row. For Appearances,
            the columns are ['playerID', 'teamID', 'yearID']
        :param debug: If true, print debug messages.
        """
        self._table_name = table_name
        self._connect_info = connect_info
        self._key_columns = key_columns
        self._debug = debug

        self._rows = None
        self._column_names = None

    def _add_row(self,r):    #  check if there's a duplicate key
        if self._rows is None:
            self._rows = []

        k = self._get_key(r)
        test_it = self.find_by_primary_key(k)

        if test_it is not None:
            raise ValueError("What part of unique is not clear?")
        else:
            self._rows.append(r)

    def __str__(self):
        result = str(type(self)) + ": name = " + self._table_name
        result += "\nconnect_info = " + str(self._connect_info)
        result += "\nKey columns = " + str(self._key_columns)

        if self._column_names is not None:
            result += "\nColumn names = " + str(self._column_names)
        if self._rows is not None:
            row_count = len(self._rows)
        else:
            row_count = 0
        result += "\nNo. of rows = " + str(row_count)

        to_print = min(5,row_count)
        for i in range(0,to_print):
            result +=  "\n" + str(dict(self._rows[i]))

        return result

    def load(self):
        fn = self._connect_info['directory'] + "/" + self._connect_info['filename']
        with open(fn,"r") as input_rows:
            creader = csv.DictReader(input_rows,dialect ='excel')
            for r in creader:
                if self._column_names is None:
                    self._column_names = list(r.keys())
                if self._rows is None:
                    self._rows = []
                self._add_row(r)

    def find_by_primary_key(self, key_fields, field_list=None):
        """
        :param key_fields: The values for the key_columns, in order, to use to find a record. For example,
            for Appearances this could be ['willite01', 'BOS', '1960']
        :param field_list: A subset of the fields of the record to return. The CSV file or RDB table may have many
            additional columns, but the caller only requests this subset.
        :return: None, or a dictionary containing the columns/values for the row.
        """
        tmp = dict(zip(self._key_columns, key_fields))
        result = self.find_by_template(tmp, field_list)
        rows = result.get_rows()
        if rows and len(rows) > 0:
            return rows[0]
        else:
            return None


    def matches_template(self,tmp,row):
        if tmp is None:
            return True
        keys = tmp.keys()
        for k in keys:
            v = row.get(k,None)
            if tmp[k] != v:
                return False
        return True

    def _project(self, rows, field_list):
        if field_list is None:
            return rows
        if rows is None:
            return None
        new_rows = []
        for r in rows:
            new_r = {f:r[f] for f in field_list}
            new_rows.append(new_r)
        return new_rows

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """
        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}. The function will return
            a derived table containing the rows that match the template.
        :param field_list: A list of requested fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A derived table containing the computed rows.
        """
        from DerivedDataTable import DerivedDataTable
        some_rows = None

        for r in self._rows:
            if self.matches_template(template,r):
                if some_rows is None:
                    some_rows = []
                some_rows.append(copy.copy(r))
        result1 = self._project(some_rows,field_list)
        final_result = DerivedDataTable("FBT:"+self._table_name, result1)
        return final_result

    def _get_key(self,row):
        result = [row[k] for k in self._key_columns]
        return result

    def insert(self, new_record):
        """
        :param new_record: A dictionary representing a row to add to the set of records. Raises an exception if this
            creates a duplicate primary key.
        :return: None
        """
        self._add_row(new_record)



    def delete_by_template(self, template):
        """
        Deletes all records that match the template.
        :param template: A template.
        :return: A count of the rows deleted.
        """
        new_rows = []
        count = 0
        for r in self._rows:
            if not self.matches_template(template,r):
                new_rows.append(copy.copy(r))
            else:
                count += 1
        self._rows = new_rows
        return count

    def delete_by_key(self, key_fields):
        """
        Deletes all records that match the template.
        :param key_fields: List containing the values for the key columns
        :return: A count of the rows deleted.
        """
        tmp = dict(zip(self._key_columns, key_fields))
        return self.delete_by_template(tmp)

    def _update_row(self, r, new_values):
        key = new_values.keys()
        new_r = copy.copy(r)
        for k in key:
            new_r[k] = new_values[k]
        return new_r

    def update_by_template(self, template, new_values):
        """
        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        count = 0
        for i in range(len(self._rows)):
            if self.matches_template(template,self._rows[i]):
                # print("here",r)
                new_r = self._update_row(self._rows[i], new_values)
                new_k = self._get_key(new_r)
                self._rows.remove(self._rows[i])
            #
                k = self.find_by_primary_key(new_k)
                if k is not None:
                    # self._add_row(r)
                    raise ValueError('ick')
                else:
                    self._add_row(new_r)
                    count += 1
        return count

    def update_by_key(self, key_fields, new_values):
        """
        :param key_fields: List of values for primary key fields
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        tmp = dict(zip(self._key_columns, key_fields))
        count = 0
        for i in range(len(self._rows)):
            if self.matches_template(tmp,self._rows[i]):
                # print("here",r)
                new_r = self._update_row(self._rows[i], new_values)
                new_k = self._get_key(new_r)
                self._rows.remove(self._rows[i])
            #
                k = self.find_by_primary_key(new_k)
                if k is not None:
                    # self._add_row(r)
                    raise ValueError('ick')
                else:
                    self._add_row(new_r)
                    count += 1
        return count
