-- Drop the database if it exists
DROP DATABASE IF EXISTS oneders_cafe;

-- Create the database
CREATE DATABASE oneders_cafe;



-- ###########   AFTER YOU RUN THE FIRST TWO COMMANDS IN ADMINER, SELECT THE oneders_cafe db FIRST THEN RUN THE REMAINING COMMANDS    ####################


-- Create the products table
CREATE TABLE products (
    productID SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    product_price DECIMAL(10,2) NOT NULL
);

-- Create the payment_type table
CREATE TABLE payment_type(
   payment_typeID SERIAL PRIMARY KEY,
   type_name TEXT NOT NULL
);

-- Create the branch table
CREATE TABLE branch (
    branchID SERIAL PRIMARY KEY,
    branch_name TEXT NOT NULL
);

-- Create the transactions table
CREATE TABLE transactions (
    orderID SERIAL PRIMARY KEY,
    branchID INT NOT NULL,
    payment_typeID INT NOT NULL,
    total_cost DECIMAL(10, 2) NOT NULL,
    order_datetime TIMESTAMP NOT NULL,
    FOREIGN KEY (branchID) REFERENCES branch(branchID),
    FOREIGN KEY (payment_typeID) REFERENCES payment_type(payment_typeID)
);

-- Create the basket table
CREATE TABLE basket (
    orderID INT NOT NULL,
    productID INT NOT NULL,
    quantity INT NOT NULL,
    PRIMARY KEY (orderID, productID),
    FOREIGN KEY (orderID) REFERENCES transactions(orderID),
    FOREIGN KEY (productID) REFERENCES products(productID)
);