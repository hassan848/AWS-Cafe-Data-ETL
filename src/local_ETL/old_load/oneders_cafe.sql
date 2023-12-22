DROP DATABASE IF EXISTS oneders_cafe;
CREATE DATABASE oneders_cafe;
use oneders_cafe;

CREATE TABLE products (
    productID INT NOT NULL AUTO_INCREMENT,
    product_name VARCHAR(255) NOT NULL,
    product_price DECIMAL(10,2) NOT NULL,
    PRIMARY KEY(productID)
);

CREATE TABLE payment_type(
   payment_typeID INT NOT NULL AUTO_INCREMENT,
   type_name VARCHAR(255) NOT NULL,
   PRIMARY KEY (payment_typeID)
);

CREATE TABLE branch (
    branchID INT NOT NULL AUTO_INCREMENT,
    branch_name VARCHAR(100) NOT NULL,
    PRIMARY KEY (branchID)
);

CREATE TABLE transactions (
    orderID INT NOT NULL AUTO_INCREMENT,
    branchID INT NOT NULL,
    payment_typeID INT NOT NULL,
    total_cost decimal (10, 2) NOT NULL,
    order_datetime datetime NOT NULL,
    PRIMARY KEY (orderID),
    FOREIGN KEY (branchID) REFERENCES branch(branchID),
    FOREIGN KEY (payment_typeID)REFERENCES payment_type(payment_typeID)
);

CREATE TABLE basket (
    orderID INT NOT NULL,
    productID INT NOT NULL,
    quantity INT NOT NULL,
    PRIMARY KEY (orderID, productID),
    FOREIGN KEY (orderID) REFERENCES transactions(orderID),
    FOREIGN KEY (productID) REFERENCES products(productID)
);

