import json
import psycopg2
from psycopg2 import sql



json_input = {"address" : "https://www.google.com ","content" : {"marks" : [{"text": "marks"},{"text": "season"},{"text": "foo"},{"text": "bar"}],"description" : "Some description"},"updated" : "2021-02-26T08:21:20+00:00","author" : {"username" : "Bob","id" : "68712648721648271"},"id" : "543435435","created" : "2021-02-25T16:25:21+00:00","counters" : {"score" : 3,"mistakes" : 
0},"type" : "main"}

name_table = "JSON_Table"



def create_table(name_table, json_input):

    tbl_json = {}
    tbl_json[name_table] = []

    tbl_json[name_table].append({"path": json_input["address"]})
    tbl_json[name_table].append({"items": [i["text"] for i in json_input["content"]["marks"]]})
    tbl_json[name_table].append({"body": json_input["content"]["description"]})
    tbl_json[name_table].append({"id": json_input["id"]})
    tbl_json[name_table].append({"author_name": json_input["author"]["username"]})
    tbl_json[name_table].append({"author_id": json_input["author"]["id"]})

    time_creation = json_input["created"].split("T")
    #print(time_creation)
    created_date = time_creation[0]
    created_time = time_creation[1]

    tbl_json[name_table].append({"created_date": created_date})
    tbl_json[name_table].append({"created_time": created_time})

    time_update = json_input["updated"].split("T")
    #print(time_update)
    update_date = time_update[0]
    update_time = time_update[1]

    tbl_json[name_table].append({"update_date": update_date})
    tbl_json[name_table].append({"update_time": update_time})

    tbl_json[name_table].append({"counters_total": json_input["counters"]["score"]+json_input["counters"]["mistakes"]})

    return tbl_json

tbl_json = create_table(name_table, json_input)
tbl_json_str = str(tbl_json).replace("'", '"')


# col_names to sql
tbl_dict = json.loads(tbl_json_str)
tbl_name = list(tbl_dict)[0]
col_names = [list(col_dict)[0] for col_dict in tbl_dict[tbl_name]]
print(col_names)

type_list = ["varchar NOT NULL", "varchar", "varchar", "varchar NOT NULL", "varchar NOT NULL", "varchar NOT NULL", "varchar NOT NULL", "varchar NOT NULL", "varchar", "varchar", "int"]
col_type = []
for i in zip(map(sql.Identifier, col_names), map(sql.SQL,type_list)):
    col_type.append(i[0] + i[1])
print(col_type)

#create table
try:
    sql_str = sql.SQL("CREATE table {} ({})").format(sql.Identifier(tbl_name), sql.SQL(',').join(col_type))
    con = psycopg2.connect("dbname=mydb host=localhost user=myuser password=mypass")
    cur = con.cursor()
    cur.execute(sql_str)
    con.commit()
except Exception as e:
    print('create error')
    print(e)

#insert value in table
try:
    path = tbl_json[name_table][0]["path"]
    items = tbl_json[name_table][1]["items"]
    body = tbl_json[name_table][2]["body"]
    id_json = tbl_json[name_table][3]["id"]
    author_name = tbl_json[name_table][4]["author_name"]
    author_id = tbl_json[name_table][5]["author_id"]
    created_date = tbl_json[name_table][6]["created_date"]
    created_time = tbl_json[name_table][7]["created_time"]
    update_date = tbl_json[name_table][8]["update_date"]
    update_time = tbl_json[name_table][9]["update_time"]
    counters_total = tbl_json[name_table][10]["counters_total"]

    items_str = str(items).replace("'",'"')
    sql_str = sql.SQL(f'INSERT INTO "{name_table}" VALUES (\'{path}\', \'{items_str}\', \'{body}\', \'{id_json}\', \'{author_name}\', \'{author_id}\', \'{created_date}\', \'{created_time}\', \'{update_date}\', \'{update_time}\', \'{str(counters_total)}\')')#.format(,  )
    con = psycopg2.connect("dbname=mydb host=localhost user=myuser password=mypass")
    cur = con.cursor()
    cur.execute(sql_str)
    con.commit()
except Exception as e:
    print('insert error')
    print(e)

#check items inserted
try:
    sql_str = sql.SQL(f'SELECT * FROM "{name_table}"')#.format(,  )
    con = psycopg2.connect("dbname=mydb host=localhost user=myuser password=mypass")
    cur = con.cursor()
    cur.execute(sql_str)
    rows = cur.fetchall()
    con.commit()
    print(rows)
except Exception as e:
    print('select error')
    print(e)