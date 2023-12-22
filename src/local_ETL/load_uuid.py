from products_and_prices_dict import get_products
from unique_branches import get_unique_branches
from normalise_clean_data import get_normalised_transactions
from datetime import datetime

import psycopg2
import os
from dotenv import load_dotenv
import uuid


# Load data from external files or functions
products = get_products()
branches = get_unique_branches()
payment_types = ['CASH', 'CARD']
transactions = get_normalised_transactions()

# Load environment variables from .env file
load_dotenv()
host_name = os.environ.get("pg_host")
database_name = os.environ.get("pg_db")
user_name = os.environ.get("pg_user")
user_password = os.environ.get("pg_pass")

def generate_unique_id():
    return str(uuid.uuid4())

def setup_db_connection():
    # Establish a connection and create a cursor
    connection = psycopg2.connect(
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
        
        # Insert products into the 'products' table
        for product in products:
            # Check if the product already exists
            check_sql = "SELECT productID FROM products WHERE product_name = %s AND product_price = %s"
            data_values = (product['name'], product['price'])

            cursor.execute(check_sql, data_values)
            existing_product_id = cursor.fetchone()
           
            if existing_product_id is None:
                # If the product doesn't exist, insert it
                product_id = generate_unique_id()
                insert_sql = """INSERT INTO products (productID, product_name, product_price) VALUES (%s, %s, %s)"""
                
                cursor.execute(insert_sql, (product_id, product['name'], product['price']))
                
                # Fetch the last inserted product_id
                product['product_id'] = product_id
                connection.commit()
                
            else:
                product['product_id'] = existing_product_id
        
        cursor.close()
        
        return products            
        
    except Exception as ex:
        print('Failed to:', ex)

# Call the function to insert products into the database
products = insert_products_db(products)

def insert_branches_db(branches):
    try:
        connection, cursor = setup_db_connection()
        
        # Insert branches into the 'branch' table
        for branch in branches:
            # Check if the branch already exists
            check_sql = "SELECT branchID FROM branch WHERE branch_name = %s"
            # data_values = (branch['branch_name'], product['price'])
            
            cursor.execute(check_sql, (branch['branch_name'],))
            existing_branch_id = cursor.fetchone()
            
            if existing_branch_id is None:
                # If the product doesn't exist, insert it
                branch_id = generate_unique_id()            
                insert_sql = """INSERT INTO branch (branchID, branch_name) VALUES (%s, %s)"""
                cursor.execute(insert_sql, (branch_id, branch['branch_name']))
                # Fetch the last inserted branch_id
                branch['branch_id'] = branch_id
                connection.commit()
            else:
                branch['branch_id'] = existing_branch_id
        
        cursor.close()
        
        return branches            
        
    except Exception as ex:
        print('Failed to:', ex)

# Call the function to insert branches into the database
branches = insert_branches_db(branches)


def insert_payment_type_db(payment_types):
    try:
        connection, cursor = setup_db_connection()
        
        # Insert payment types into the 'payment_type' table
        for i in range(len(payment_types)):
            # Check if the payment_type already exists
            check_sql = "SELECT payment_typeID FROM payment_type WHERE type_name = %s"
            cursor.execute(check_sql, (payment_types[i],))
            existing_type_id = cursor.fetchone()
            
            if existing_type_id == None:
                type_id = generate_unique_id()
                sql = """INSERT INTO payment_type (payment_typeID, type_name) VALUES (%s, %s)"""
                cursor.execute(sql, (type_id, payment_types[i]))
                # Fetch the last inserted payment_type_id
                payment_types[i] = {'payment_type_id': type_id, 'type_name': payment_types[i]}
                connection.commit()
            else:
                payment_types[i] = {'payment_type_id': existing_type_id, 'type_name': payment_types[i]}
        cursor.close()
        
        return payment_types            
        
    except Exception as ex:
        print('Failed to:', ex)

# Call the function to insert payment types into the database
payment_types = insert_payment_type_db(payment_types)



# Inserting transaction data into the 'transactions' table
def insert_transactions_db(transactions):
    try:
        connection, cursor = setup_db_connection()
        
        for transaction in transactions:
            # Get branch_id and payment_type_id using the fetched values
            branch_id = next(branch['branch_id'] for branch in branches if branch['branch_name'] == transaction['location'])
            payment_type_id = next(type['payment_type_id'] for type in payment_types if type['type_name'] == transaction['payment_type'])
            total_cost = transaction['total']
        
            # Format the date as needed
            date_object = datetime.strptime(transaction['date'], '%d/%m/%Y %H:%M')
            formatted_date = date_object.strftime('%Y-%m-%d %H:%M')
            
            # Insert transaction data into the 'transactions' table
            order_id = generate_unique_id()
            sql = """INSERT INTO transactions (orderID, branchID, payment_typeID, total_cost, order_datetime) VALUES (%s, %s, %s, %s, %s)"""
            data = (order_id, branch_id, payment_type_id, total_cost, formatted_date)
            cursor.execute(sql, data)
            # Fetch the last inserted order_id
            transaction['order_id'] = order_id
            connection.commit()
            
        cursor.close()
        return transactions
    
    except Exception as ex:
        print('Failed to:', ex)

# Call the function to insert transactions into the database
transactions = insert_transactions_db(transactions)

# Inserting basket data into the 'basket' table
def insert_basket_db(transactions):
    try:
        connection, cursor = setup_db_connection()
        for transaction in transactions:
            order_id = transaction["order_id"]
            for current_product in transaction["basket"]:
                product_id = next(product["product_id"] for product in products if product["name"] == current_product["product"])
                quantity = current_product["quantity"]
                # Insert basket data into the 'basket' table
                sql = """INSERT INTO basket (orderID, productID, quantity) VALUES (%s, %s, %s)"""
                data = (order_id, product_id, quantity)
                cursor.execute(sql, data)
                connection.commit()
        cursor.execute('select * from products;')
        returned_products = cursor.fetchone()
        print(returned_products)
        cursor.close()
    except Exception as ex:
        print('Failed to:', ex)

# Call the function to insert basket data into the database
insert_basket_db(transactions)

