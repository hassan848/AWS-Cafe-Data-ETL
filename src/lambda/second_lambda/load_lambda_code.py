from connect_to_db import *
from generate_sql_db import *
#from my_utility_functions import *
# import os
import csv
from io import StringIO
import ast
from datetime import datetime
import uuid
import json

# ssm_param_name = os.environ.get('SSM_PARAM_NAME') or 'NOT_SET'
ssm_param_name = 'oneders_redshift_settings'

#ohh. so the lambada_handler houses everything?
def lambda_handler(event, context):
    print('lambda_handler: starting')
    # try
    
    s3 = boto3.client('s3')
    print(event)
    # Get the bucket name and key for the uploaded file
    parsed_message = json.loads(event['Records'][0]['body'])
    source_bucket = parsed_message['Records'][0]['s3']['bucket']['name']
    key = parsed_message['Records'][0]['s3']['object']['key']
    print(source_bucket, key)
    
    # Get the file object from S3
    file_obj = s3.get_object(Bucket=source_bucket, Key=key)
    file_content = file_obj['Body'].read().decode('utf-8')
    #transactions = csv.DictReader(StringIO(file_content))
    transactions = list(csv.DictReader(StringIO(file_content)))

    
    # transactions = list(csv.DictReader(StringIO(file_content)))
    # transactions = []
    #to change the string vlaue for the key basket to a list of dicts
    for row in transactions:
        row['basket'] = ast.literal_eval(row['basket'])
        
    print('First Transaction: ', transactions[0])

    # inspect event
    #bucket_name, file_path = get_details(event)
    #print(f'lambda_handler: event: file=${file_path}, bucket_name=${bucket_name}')

    # load file from s3
    #file_object = get_file(bucket_name, file_path)
    # process file as csv
    #csv_file = do_something(file_object)
    # transform the data /clean it / reorganize
    #dict_or_list_of_data = my_transform_function(csv_file)

    # connection
    redshift_details = get_ssm_param(ssm_param_name)
    connection, cursor = open_sql_database_connection_and_cursor(redshift_details)

    # run some sql
    create_db_tables(connection, cursor)

    # run some sql
    #save_my_data(conn, cur, dict_or_list_of_data)
    products = get_product_prices(transactions)  # acutally just unique_produts_list from below
    branches = get_unique_branches(transactions)
    payment_types = ['CASH', 'CARD']
    #print(f'lambda_handler: done, file=${file_path}')
    
    # Call the function to insert products into the database
    products = insert_products_db(connection, cursor, products)
    
    # Call the function to insert branches into the database
    branches = insert_branches_db(connection, cursor, branches)
    
    # Call the function to insert payment types into the database
    payment_types = insert_payment_type_db(connection, cursor, payment_types)
    
    # Call the function to insert transactions into the database
    transactions = insert_transactions_db(connection, cursor, transactions, branches, payment_types)
    
    # Call the function to insert basket data into the database
    insert_basket_db(connection, cursor, transactions, products)
    #return connection,cursor
    
    # insert_sql = """INSERT INTO products (productID,product_name, product_price) VALUES (%s, %s,%s)"""
    # cursor.execute(insert_sql, ("renmhhanjfjrf", "test_name", 0.90))
    # connection.commit()
    
    # check_db_sql = 'SELECT * FROM products'
    # cursor.execute(check_db_sql)
    # product_results = cursor.fetchone()
    # print(product_results)
    cursor.close()
    # catch Exception as whoopsy
    # ...exception reporting
    #print(f'lambda_handler: boom, error=${whoopsy}, file=${file_path}')


def insert_products_db(connection, cursor, products):
    print(products)
    # Insert products into the 'products' table
    for product in products:
        # Check if the product already exists
        check_sql = "SELECT productID FROM products WHERE product_name = %s AND product_price = %s"
        data_values = (product['name'], product['price'])

        cursor.execute(check_sql, data_values)
        existing_product_id = cursor.fetchone()
        
        if existing_product_id is None:
            product_id = str(uuid.uuid4())
            #data_values = data_values + (product_id,)
            # If the product doesn't exist, insert it
            insert_sql = """INSERT INTO products (productID,product_name, product_price) VALUES (%s, %s,%s)"""
            cursor.execute(insert_sql, (product_id, product['name'], product['price']))
            
            # # Fetch the last inserted product using IDENT_CURRENT
            # cursor.execute("SELECT IDENT_CURRENT('products')")
            product['product_id'] = product_id
            connection.commit()
            
        else:
            product['product_id'] = existing_product_id
    
    # cursor.close()
    
    return products            
        
def insert_branches_db(connection, cursor, branches):
    # connection, cursor = setup_db_connection()
    # Insert branches into the 'branch' table
    for branch in branches:
        # Check if the branch already exists
        check_sql = "SELECT branchID FROM branch WHERE branch_name = %s"
        # data_values = (branch['branch_name'], product['price'])
        
        cursor.execute(check_sql, (branch['branch_name'],))
        existing_branch_id = cursor.fetchone()
        
        if existing_branch_id is None:
            # If the branch doesn't exist, insert it
            branch_id= str(uuid.uuid4())            
            sql = """INSERT INTO branch (branchID,branch_name) VALUES (%s,%s)"""
            cursor.execute(sql, (branch_id,branch['branch_name']))
            
            # # Fetch the last inserted branch_id using IDENT_CURRENT
            # cursor.execute("SELECT IDENT_CURRENT('branch')")
            #adding a new key-value pair to the branches list
            branch['branch_id'] = branch_id
            connection.commit()
        else:
            branch['branch_id'] = existing_branch_id
    
    # cursor.close()
    
    return branches
    


# def get_sequence_name(cursor, table_name, column_name):
#     cursor.execute(f"SELECT sequence_name FROM information_schema.columns WHERE table_name='{table_name}' AND column_name='{column_name}'")
#     sequence_name = cursor.fetchone()
#     if sequence_name:
#         sequence_name3 = sequence_name[0]
#         return sequence_name3
#     else:
#         return None

   

def insert_payment_type_db(connection, cursor, payment_types):
    print(payment_types)
    # Insert payment types into the 'payment_type' table
    for i in range(len(payment_types)):
        # Check if the payment_type already exists
        check_sql = "SELECT payment_typeID FROM payment_type WHERE type_name = %s"
        cursor.execute(check_sql, (payment_types[i],))
        existing_type_id = cursor.fetchone()
        
        if existing_type_id == None:
            payment_id= str(uuid.uuid4())
            sql = """INSERT INTO payment_type (payment_typeID,type_name) VALUES (%s,%s)"""
            cursor.execute(sql, (payment_id,payment_types[i]))
            
            # sequence_name2 = get_sequence_name(cursor, "payment_type", "payment_typeID")
            
            # Fetch the last inserted payment_typeID using IDENT_CURRENT
            #cursor.execute(f"SELECT currval('{sequence_name2}')")
            
            #cursor.execute("SELECT IDENT_CURRENT('payment_type')")
            last_inserted_id = payment_id
            #ok. so just chainging the payment_types list created above, payment_types=["CASH","CARD"] to a list of 2 dicts as described below
            payment_types[i] = {'payment_type_id': last_inserted_id, 'type_name': payment_types[i]}
            connection.commit()
        else:
            payment_types[i] = {'payment_type_id': existing_type_id, 'type_name': payment_types[i]}
    
    # cursor.close()
    
    return payment_types            
    
def insert_transactions_db(connection, cursor, transactions, branches, payment_types):
    for transaction in transactions:
        # Get branch_id and payment_type_id using the fetched values
        branch_id = next(branch['branch_id'] for branch in branches if branch['branch_name'] == transaction['location'])
        payment_type_id = next(type['payment_type_id'] for type in payment_types if type['type_name'] == transaction['payment_type'])
        total_cost = float(transaction['total'])
    
        # Format the date as needed
        date_object = datetime.strptime(transaction['date'], '%d/%m/%Y %H:%M')
        formatted_date = date_object.strftime('%Y-%m-%d %H:%M')
        
        # Insert transaction data into the 'transactions' table
        sql = """INSERT INTO transactions (orderID,branchID, payment_typeID, total_cost, order_datetime) VALUES (%s,%s, %s, %s, %s)"""
        orderid= str(uuid.uuid4())
        data = (orderid,branch_id, payment_type_id, total_cost, formatted_date)
        cursor.execute(sql, data)
        
        # Fetch the last inserted order_id using IDENT_CURRENT
        #cursor.execute("SELECT IDENT_CURRENT('transactions')")
        #ohh. so we're adding a new key value pair for each record in the csv
        transaction['order_id'] = orderid
        connection.commit()
    print('Committed transactions')
        
    # cursor.close()
    return transactions


# Inserting basket data into the 'basket' table
def insert_basket_db(connection, cursor, transactions, products):
    # connection, cursor = setup_db_connection()
    for transaction in transactions:
        order_id = transaction["order_id"]
        for current_product in transaction["basket"]:   #remeber, the value for key basket is a list of dicts so current_product is a dict
            product_id = next(product["product_id"] for product in products if product["name"] == current_product["product"])  #ohh so this simply runs through our 'yellow pages' called products. We simply looks at our current pruduct and then search through our documentation for the product id assosiated with it. rmemeber, our products yellow pages is distint and made up by us to help find product ids as a referecing tool.
            quantity = current_product["quantity"]
            # Insert basket data into the 'basket' table
            sql = """INSERT INTO basket (orderID, productID, quantity) VALUES (%s, %s, %s)"""
            data = (order_id, product_id, quantity)
            cursor.execute(sql, data)
            connection.commit()
    print("i like icecream")    
    #cursor.close()



def get_product_prices(transactions):
    unique_products_dict = {}

    # Iterate through the data_list and update the unique_products_dict
    for entry in transactions:
        basket = entry.get('basket', [])
        for product in basket:
            product_name = product['product']
            if product_name not in unique_products_dict:
                unique_products_dict[product_name] = {'name': product_name, 'price': product['price']}

    # Convert the values of the unique_products_dict to a list of dictionaries
    unique_products_list = list(unique_products_dict.values())
    print(unique_products_list)
    return unique_products_list

def get_unique_branches(transactions):
    branches = []
    branch_names = []
    for record in transactions:
        if record['location'] not in branch_names:
            branch_names.append(record['location'])
            branches.append({'branch_name': record['location']})
    return branches
    #so branch names is keeping track of all unique branches added to branches so far. I've read this way of doing it improves speed.
    
