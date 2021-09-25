import sqlite3
import random
# db = sqlite3.connect('database2_new.db')
db = sqlite3.connect('DB22.db')
cursor = db.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("tables:")
print(tables)
detail = {}

# fill detail object with the name of table and the number of rows
for table_name in tables:
    query = "SELECT Count(*) from " + table_name[0] + ";"
    cursor.execute(query)
    # print(cursor.fetchall())
    detail[table_name[0]] = cursor.fetchall()[0][0]

Table = []
TotalNumberofRows = 0
for item in detail:
    TotalNumberofRows = TotalNumberofRows + detail[item]
    Table.append(item)
    
# primary key and foreign key details
tablecolumn = {
    'vehicleOrderInfo': {'vehicleInfo': 'vehicleCode', 'vehicleOrders': 'vehicleOrderNumber'},
    'vehicleTypes': {'vehicleInfo': 'vehicleType'},
    'vehicleOrders': {'buyers': 'buyerNumber', 'vehicleOrderInfo': 'vehicleOrderNumber'},
    'paymentInfo': {'buyers': 'buyerNumber'},
    'vehicleInfo': {'vehicleTypes': 'vehicleType', 'vehicleOrderInfo': 'vehicleCode'},
    'warehouses': {'staffs': 'officeCode'},
    'staffs': {'buyers': 'staffNumber'},
    'buyers': {'vehicleOrders': 'buyerNumber', 'paymentInfo': 'buyerNumber'}
}


selectivitytable=[round(random.uniform(0, 1)/10,4) for _ in range(len(detail))]
print("selectivitytable")
print(selectivitytable)



def selectivityUtil(table1, table2, column1):
    # selectivtiy of join is card(join)/max(card(a),card(b):
    sql = 'SELECT  count(DISTINCT ' + table1 + '.' + column1 + ' )  from ' + table1 + "  JOIN " + table2 + " ON " + table1 + '.' + column1 + " = " + table2 + '.' + column1 + ";"
    #print(sql)
    cursor.execute(sql)
    result = cursor.fetchall()
    j = result[0][0]

    sql = "Select count(*) from " + table1 + ";"
    #print(sql)
    cursor.execute(sql)
    result = cursor.fetchall()
    t1 = result[0][0]
    #print(t1)

    sql = "Select count(*) from " + table2 + ";"
    cursor.execute(sql)
    result = cursor.fetchall()
    t2 = result[0][0]

    sel = round(j / max(t1, t2), 4)
    #print(sel)
    Nt = round(t1 * sel / TotalNumberofRows, 4)
    #print(Nt)
    return Nt

# Control Local Processing Cost
def CLPC(database):
    sel = []
    for table1 in database:
        for table2 in database[table1]:
            column = database[table1][table2]
            sel.append(selectivityUtil(table1, table2, column))
            #print(table1, 'join', table2, ':', column, selectivityUtil(table1, table2, column))
    return max(sel)


ClpcOfDatabase = CLPC(tablecolumn)

print("Details of the Database:\n")
print("Details of Tables:", detail)
print("Sum of all Tuples", TotalNumberofRows)
print('CLPC of the database:', ClpcOfDatabase)

print("-------------------------------------------------")
# sql = 'SELECT COUNT(DISTINCT officeCode) FROM employees;'
# cursor.execute(sql)
# result = cursor.fetchall()
# print(result)

# sql = 'SELECT COUNT(*) FROM employees;'
# cursor.execute(sql)
# result = cursor.fetchall()
# print(result)

# cursor.close()
# db.close()