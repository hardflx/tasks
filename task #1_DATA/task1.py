import re
import os
import json
import mysql.connector

base = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(base, "task1_d.json")
with open(path, "r", encoding="utf-8") as f:
    input_string = f.read()



input_str = input_string.replace('=>', ':')
input_str = re.sub(r'({|, ):(\w+)', r'\1"\2"', input_str)
books = json.loads(input_str)

exchange_rate = 1.2
field_key = 'price'

for book in books:
    if field_key in book:
        try:
            if "$" in book[field_key]:
                book[field_key] = float(book[field_key].replace("$", ""))
            elif "€" in book[field_key]:
                book[field_key] = float(book[field_key].replace("€", ""))
                book[field_key] = round((book[field_key] * exchange_rate) , 2)
        except ValueError:
            print(f'impossible to transform to float')

try:
    con = mysql.connector.connect(
        user = 'root',
        password = 'placeholder',
        host = 'localhost',
        port = 3306,
        database = 'tasks'
    )
    if con.is_connected():
        print("connection successful")
except Exception as e:
    print("Connection failed")

cur = con.cursor()

for item in books:
    values = (item['id'], item['title'],item['author'], item['genre'], item['publisher'],item['year'],item['price'])
    sql = "insert into books(BookID,BookTitle,BookAuthor,BookGenre,BookPublisher,BookReleaseYear,BookPriceInUSD)VALUES(%s,%s,%s,%s,%s,%s,%s)"
    cur.execute(sql,values)

con.commit()
cur.close()
con.close()

