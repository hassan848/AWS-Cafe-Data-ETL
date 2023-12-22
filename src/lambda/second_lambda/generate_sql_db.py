from connect_to_db import *

def create_db_tables(connection, cursor) -> bool:
    print('create_db_tables started')
    try:
        # #Drop tables if they exist
        # drop_tables_sql = (
        #     "DROP TABLE IF EXISTS basket;",
        #     "DROP TABLE IF EXISTS transactions;",
        #     "DROP TABLE IF EXISTS branch;",
        #     "DROP TABLE IF EXISTS payment_type;",
        #     "DROP TABLE IF EXISTS products;"
        # )
        
        # for command in drop_tables_sql:
        #     cursor.execute(command)
        
        # # Create sequences
        # cursor.execute("CREATE SEQUENCE products_productid_seq;")
        # cursor.execute("CREATE SEQUENCE payments_typeid_seq;")
        # cursor.execute("CREATE SEQUENCE branch_branchid_seq;")
        # cursor.execute("CREATE SEQUENCE transactions_orderid_seq;")
        
        # Create tables
        create_tables_sql = ("""
            CREATE TABLE IF NOT EXISTS products (
                productID VARCHAR(36) PRIMARY KEY,
                product_name VARCHAR(100) NOT NULL,
                product_price DECIMAL(10,2) NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS payment_type(
                payment_typeID VARCHAR(36) PRIMARY KEY,
                type_name VARCHAR(100) NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS branch (
                branchID VARCHAR(36) PRIMARY KEY,
                branch_name VARCHAR(100) NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS transactions (
                orderID VARCHAR(36) PRIMARY KEY,
                branchID VARCHAR(36) NOT NULL,
                payment_typeID VARCHAR(36) NOT NULL,
                total_cost DECIMAL(10, 2) NOT NULL,
                order_datetime TIMESTAMP NOT NULL,
                FOREIGN KEY (branchID) REFERENCES branch(branchID),
                FOREIGN KEY (payment_typeID) REFERENCES payment_type(payment_typeID)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS basket (
                orderID VARCHAR(36) NOT NULL,
                productID VARCHAR(36) NOT NULL,
                quantity INT NOT NULL,
                PRIMARY KEY (orderID, productID),
                FOREIGN KEY (orderID) REFERENCES transactions(orderID),
                FOREIGN KEY (productID) REFERENCES products(productID)
            );
            """
            )
        # note to self: i dont like this basket table. it has as a key, the unique pairing of orderid and productid
        for command in create_tables_sql:
            cursor.execute(command)

        connection.commit()
        print('...committed')
        print('create_db_tables done')
        return True
    except Exception as ex:
        print(f'create_db_tables failed to generate table/s:\n{ex}')
        return False