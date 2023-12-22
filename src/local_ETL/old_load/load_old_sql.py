from products_and_prices_dict import get_products
from unique_branches import get_unique_branches
from normalise_clean_data import get_normalised_transactions
from datetime import datetime

import pymysql
import os
from dotenv import load_dotenv

products = get_products()
branches = get_unique_branches()
payment_types = ['CASH', 'CARD']
transactions = get_normalised_transactions()

# Load environment variables from .env file
load_dotenv()
host_name = os.environ.get("mysql_host")
database_name = os.environ.get("mysql_db")
user_name = os.environ.get("mysql_user")
user_password = os.environ.get("mysql_pass")

def setup_db_connection():
    
    connection = pymysql.connect(
        host=host_name,
        user=user_name,
        password=user_password,
        database=database_name
    )
    cursor = connection.cursor()
    return connection, cursor

def insert_products_db(products):
    try:
        connection, cursor = setup_db_connection()
        
        for product in products:
            sql = """INSERT products (product_name, product_price) VALUES (%s, %s)"""
            data_values = (product['name'], product['price'])
            
            cursor.execute(sql, data_values)
            connection.commit()
            product['product_id'] = cursor.lastrowid
        
        cursor.close()
        
        return products            
        
    except Exception as ex:
        print('Failed to:', ex)
        
products = insert_products_db(products)

def insert_branches_db(branches):
    try:
        connection, cursor = setup_db_connection()
        
        for branch in branches:
            sql = """INSERT branch (branch_name) VALUES (%s)"""    
            cursor.execute(sql, branch['branch_name'])
            connection.commit()
            branch['branch_id'] = cursor.lastrowid
        
        cursor.close()
        
        return branches            
        
    except Exception as ex:
        print('Failed to:', ex)
        
branches = insert_branches_db(branches)

def insert_payment_type_db(payment_types):
    try:
        connection, cursor = setup_db_connection()
        
        for i in range(len(payment_types)):
            sql = """INSERT payment_type (type_name) VALUES (%s)"""    
            cursor.execute(sql, payment_types[i])
            connection.commit()
            payment_types[i] = {'payment_type_id': cursor.lastrowid, 'type_name': payment_types[i]}
            # branch['branch_id'] = cursor.lastrowid
        
        cursor.close()
        
        return payment_types            
        
    except Exception as ex:
        print('Failed to:', ex)
        
payment_types = insert_payment_type_db(payment_types)


# Inserting transaction data to database
def insert_transactions_db(transactions):
    try:
        connection, cursor = setup_db_connection()
        
        for transaction in transactions:
            branch_id = next(branch['branch_id'] for branch in branches if branch['branch_name'] == transaction['location'])
            payment_type_id = next(type['payment_type_id'] for type in payment_types if type['type_name'] == transaction['payment_type'])
            total_cost = transaction['total']
        
            # Format the date as needed
            date_object = datetime.strptime(transaction['date'], '%d/%m/%Y %H:%M')
            formatted_date = date_object.strftime('%Y-%m-%d %H:%M')
            
            sql = """INSERT transactions (branchID, payment_typeID, total_cost, order_datetime) VALUES (%s, %s, %s, %s)"""
            data = (branch_id, payment_type_id, total_cost, formatted_date)
            cursor.execute(sql, data)
            connection.commit()
            
            transaction['order_id'] = cursor.lastrowid
            
        cursor.close()
        return transactions
    
    except Exception as ex:
        print('Failed to:', ex)

transactions = insert_transactions_db(transactions)

def insert_basket_db(transactions):
    try:
        connection, cursor = setup_db_connection()
        for transaction in transactions:
            order_id= transaction["order_id"]
            for current_product in transaction["basket"]:
                product_id= next(product["product_id"] for product in products if product["name"] == current_product["product"])
                quantity= current_product["quantity"]
                sql = """INSERT basket (orderID,productID,quantity) VALUES (%s,%s,%s)"""
                data=(order_id,product_id,quantity)
                cursor.execute(sql,data)
                connection.commit()
        cursor.close()
    except Exception as ex:
        print('Failed to:', ex)
        
        
insert_basket_db(transactions)

# for transaction in transactions:
#     print(transaction)
# print(transactions[3])

#ALTER TABLE tablename AUTO_INCREMENT = 1

# SELECT basket.orderID, products.product_name, basket.quantity
# FROM basket
# INNER JOIN products ON basket.productID=products.productID
# WHERE basket.quantity > 1

#INSERT transactions (branchID, payment_typeID, total_cost, order_datetime) VALUES (1, 2, 2.30, format(2021-08-25 09:00, 'yyyy-mm-dd hh:mm'))

############# remove everything from db ###############
# delete from basket;

# delete from transactions;
# ALTER TABLE transactions AUTO_INCREMENT = 1;
# delete from branch;
# ALTER TABLE branch AUTO_INCREMENT = 1;

# delete from payment_type;
# ALTER TABLE payment_type AUTO_INCREMENT = 1;

# delete from products;
# ALTER TABLE products AUTO_INCREMENT = 1;