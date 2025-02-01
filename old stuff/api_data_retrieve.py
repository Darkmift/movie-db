import requests
import mysql.connector
from common import headers, VALID_TABLES, DATABASE_NAME

#Every function <funcX> is responsiable to populate one table
#Example to how insert: https://www.w3schools.com/python/python_mysql_insert.asp


mydb = mysql.connector.connect(
    host="127.0.0.2",
    port="3333",
    user="natanel",
    password="nat72836",
    database={DATABASE_NAME}
)


def func0(headers):
    url = "someurl"

    response = requests.get(url, headers=headers)
    print(response.text)


def func1(headers):
    url = "someurl"

    response = requests.get(url, headers=headers)
    print(response.text)


def func2(headers):
    url = "someurl"

    response = requests.get(url, headers=headers)
    print(response.text)

def func3(headers):
    url = "someurl"

    response = requests.get(url, headers=headers)
    print(response.text)

def func4(headers):
    url = "someurl"

    response = requests.get(url, headers=headers)
    print(response.text)


def insert_multiple_records(table_name, data_rows):
    """
    Insert multiple records into a valid table from our allow list.
    
    :param table_name: Name of the table to insert the data
    :param data_rows: A list of tuples, where each tuple represents one row of data
    """

    #Prevent SQL injection by checking that the table name is valid using a whitelist
    # 1) Ensure the table is in the allow list
    if table_name not in VALID_TABLES:
        raise ValueError(f"Table '{table_name}' is not in the allow list and cannot be used.")

    # 2) Get the columns from our allow list
    columns = VALID_TABLES[table_name]
    
    # 4) For columns, we simply join them as plain text (per your request)
    #    Because they come from our strict VALID_TABLES, we assume they're safe.
    columns_str = ", ".join(columns)

    # 5) Create placeholders for each column
    placeholders = ", ".join(["%s"] * len(columns))
    
    # 6) Build the final INSERT statement
    insert_query = (
        f"INSERT INTO {table_name} ({columns_str}) "
        f"VALUES ({placeholders})"
    )
    
    # 7) Execute the insert
    cursor = mydb.cursor()
    cursor.executemany(insert_query, data_rows)
    mydb.commit()

    print(f"{cursor.rowcount} records inserted into table '{table_name}'.")
    cursor.close()


def main(): 
    print("hi")
    insert_multiple_records("new_table_test", [("11", "John", "2021-08-09"), ("21", "Peter", "2021-08-09"), ("31", "Amy", "2021-08-09"), ("4", "Hannah", "2021-08-09"), ("15", "Michael", "2021-08-09")])


if __name__ == "__main__":
    main()


