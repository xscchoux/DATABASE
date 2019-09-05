import csv
import json
import pymysql.cursors
import pandas as pd

class TenGreatestHittersCSV():
    def __init__(self):
        self._rows = []
        self._newrows = []

    def merge_two_dicts(self, x, y):
        """Given two dicts, merge them into a new dict as a shallow copy."""
        z = x.copy()
        z.update(y)
        return z

    def openfile(self):
        with open('../Data/Batting.csv', 'r+', newline='') as csvfile, open('../Data/people.csv', 'r+', newline='') as people:
            readpeople = csv.DictReader(people, dialect='excel')
            readbatting = csv.DictReader(csvfile, dialect='excel')
            readp = list(readpeople)
            readb = list(readbatting)
            keys = ['nameFirst', 'nameLast']
            keys2 = ['yearID', 'AB', 'H']
            for row1 in readp:
                dict01 = dict()
                for row2 in readb:
                    if row1['playerID'] == row2['playerID']:
                        dict01['playerID'] = row2['playerID']
                        dict01['yearID'] = dict01.get('yearID', []) + [row2['yearID']]
                        dict01['AB'] = dict01.get('AB', 0) + float(row2['AB'])
                        dict01['H'] = dict01.get('H', 0) + float(row2['H'])
                        for kk in keys:
                            dict01[kk] = row1[kk]
                # print(dict01)
                self._rows.append(dict01)
            # print(self._rows)
# add new columns
            for row in self._rows:
                if row:
                    xx = max(map(float,row['yearID']))
                else:
                    continue
                if int(xx) < 1960:
                    continue
                xxx = row.pop('AB')
                if float(xxx) <= 200:
                    continue
                row['last_year'] = xx
                row['first_year'] = min(map(float,row['yearID']))
                row.pop('yearID')
                row['career_at_bats'] = xxx
                row['career_hits'] = row.pop('H')
                row['career_average'] = row['career_hits'] /row['career_at_bats']
                self._newrows.append(row)
            # print(self._newrows)
#     # sort
            from functools import cmp_to_key
            self._newrows.sort(key=cmp_to_key(lambda x,y: y['career_average']-x['career_average'] ))
            print("\n\n", self._newrows[:10])
            return self._newrows[:10]


class TenGreatestHittersSQL():

    # Connect to the database over the network. Use the connection
    # to send commands to the DB.
    default_cnx = pymysql.connect(host='localhost',
                                 user='root',
                                 password='root',
                                 db='lahman',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)


    def run_q(self,q, args=None, fields= None, fetch=True, cnx=None):
        """

        :param q: An SQL query string that may have %s slots for argument insertion.
        :param args: A tuple of values to insert in the %s slots.
        :param fetch: If true, return the result.
        :param cnx: A database connection. May be None
        :return: A result set or None.
        """

        if cnx is None:
            cnx =  TenGreatestHittersSQL.default_cnx

        if fields:
            q = q.format(",".join(fields))

        cursor=cnx.cursor()             # Just ignore this for now.
        print ("Query = ", q)

        cursor.execute(q, args)               # Execute the query.
        r = cursor.fetchall()           # Return all elements of the result.

        return r

    def run(self):
        q = "SELECT Batting.playerID, \
                    (SELECT People.nameFirst FROM People WHERE People.playerID=Batting.playerID) as first_name, \
                    (SELECT People.nameLast FROM People WHERE People.playerID=Batting.playerID) as last_name, \
                    sum(Batting.h)/sum(batting.ab) as career_average, \
                    sum(Batting.h) as career_hits, \
                    sum(Batting.ab) as career_at_bats,\
                    min(Batting.yearID) as first_year, \
                    max(Batting.yearID) as last_year\
                    FROM \
                    Batting \
                    GROUP BY \
                    playerId \
                    HAVING \
                    career_at_bats > 200 AND last_year >= 1960 \
                    ORDER BY \
                    career_average DESC \
                    LIMIT 10;"

        result = self.run_q(q)
        return result
        # print("Result=", json.dumps(result, indent=2))
# if __name__ == "__main__":
#     s = TenGreatestHittersTest()
#     print(s.openfile())
#     readpeople = csv.DictReader(people, delimiter=',', quotechar='"')
#     keys = ['playerID','nameFirst','nameLast']
#     for row in readpeople:
#         new_dict = {key:value for key, value in row.items() if key in keys}
#         if not new_dict in peoplelist:
#             peoplelist.append(new_dict)
#     print(peoplelist)
#
#     # delete unnecessary keys and rows
#     keys2 = ['playerID','yearID','AB','H']
#     new_bat = []
#     read = csv.DictReader(csvfile, delimiter=',', quotechar='"')
#     list01 = list(read)
#     for row in list01:
#         bat_dict = {}
#         for k in keys2:
#             bat_dict[k] = row[k]
#         new_bat.append(bat_dict)
#
#     # group by playerID
#     list_of_dicts = new_bat
#     key = 'playerID'
#     d = {}
#     for dct in list_of_dicts:
#         if dct[key] not in d:
#             d[dct[key]] = {}
#         for k, v in dct.items():
#             if k != key:
#                 if k == 'yearID' and dct[k] is not None:
#                     d[dct[key]][k] = d[dct[key]].get(k,[]) + [float(dct[k])]
#                     print([float(dct[k])], dct['H'])
#                 elif dct[k] is not None:
#                     if k not in d[dct[key]]:
#                         d[dct[key]][k] = float(v)
#                     else:
#                         d[dct[key]][k] += float(v)
#                 else:
#                     break
#     print("d=", d)
#     final_list = []
#     for k, v in d.items():
#         temp_d = {key: k}
#         for k2, v2 in v.items():
#             temp_d[k2] = v2
#         final_list.append(temp_d)
#     print(final_list)
# # [{'playerID': 'addybo01', 'yearID': ['1871'], 'AB': 118, 'H': 32}, {'playerID': 'allisar01', 'yearID': ['1871', '1872']...
#
# # create career_hits, career_at_bats, career_average, first_year, last_year
#     listt = []
#     for row in final_list:
#         xx = max(row['yearID'])
#         if int(xx) < 1960:
#             continue
#         xxx = row.pop('AB')
#         if float(xxx) == 0.0:
#             continue
#         row['last_year'] = xx
#         row['first_year'] = min(row['yearID'])
#         row.pop('yearID')
#         row['career_at_bats'] = xxx
#         row['career_hits'] = row.pop('H')
#         row['career_average'] = row['career_hits'] /row['career_at_bats']
#         listt.append(row)
#     print(listt)
#
# # merge
#     res = []
#     for i1 in listt:
#         for i2 in peoplelist:
#             if i1['playerID'] == i2['playerID']:
#                 temp = merge_two_dicts(i1, i2)
#                 res.append(temp)
#     print(res)
# # sort
#     from functools import cmp_to_key
#     res.sort(key=cmp_to_key(lambda x,y: y['career_average']-x['career_average'] ))
#     print("\n\n",res[:5])




