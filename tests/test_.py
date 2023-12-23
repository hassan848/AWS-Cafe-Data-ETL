import csv
import unittest

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import products_and_prices_dict

#Note: This test is prediciated on the csv file having headers already present, as listed in the self.first_row below
class testloaddata(unittest.TestCase):
    
    def setUp(self):
        self.first_row = {"date":"25/08/2021 09:00",
                     "location":"Chesterfield",
                     "name": "Richard Copeland",
                     "basket":"Regular Flavoured iced latte - Hazelnut - 2.75, Large Latte - 2.45",
                     "total":"5.2",
                     "payment_type":"CARD",
                     "card_number":"5494173772652516"
        }
        self.filename = "C:\\Users\\saadn\\OneDrive\\Documents\\bootcamp stuff\\oneders\\chesterfield_25-08-2021_09-00-00.csv"
        
    
    def test_loading_data(self):
        with open(self.filename,"r") as f:
            reader = csv.DictReader(f)
            data= list(reader)
        self.assertEqual(data[0],self.first_row)
        
    
    def test_extracting_basket_value(self):
        all_4th_values = products_and_prices_dict.extract_4th_column(self.filename)
        expected_outcome = ["Regular Flavoured iced latte - Hazelnut - 2.75, Large Latte - 2.45"]
        actual_outcome= [all_4th_values[0]]
        
        self.assertEqual(expected_outcome,actual_outcome)
        
    #to run tests, one must enter  pytest tests/test_.py in the termal from the onders directory    
        
        
        
