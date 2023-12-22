import csv
def load_data(filename):
    with open(filename, 'r') as f:
        return list(csv.DictReader(f))

# # Remember, row is a dictionary   {"date":"25/08/2021 09:00",
#                      "location":"Chesterfield",
#                      "name": "Richard Copeland",
#                      "basket":"Regular Flavoured iced latte - Hazelnut - 2.75, Large Latte - 2.45",
#                      "total":"5.2",
#                      "payment_type":"CARD",
#                      "card_number":"5494173772652516"
#         }
def extract_4th_column(filename):
    data = load_data(filename)
    fourth_column_values = [row[list(row.keys())[3]] for row in data]
    return fourth_column_values

#now need to sepearate the list on ont the commmas,
def separate_drinks(drinks_list):
    new_list = []
    for item in drinks_list:
        if isinstance(item, str):  # Make sure the item is a string
            drinks = item.split(", ")
            new_list.extend(drinks)
    return new_list



def remove_duplicates_ordered(original_list):
    seen = set()
    new_list = []
    for item in original_list:
        if item not in seen:
            seen.add(item)
            new_list.append(item)
    return new_list

lst_of_products_and_prices =(remove_duplicates_ordered(separate_drinks(extract_4th_column("chesterfield_25-08-2021_09-00-00.csv"))))


product_dict = {}
for item in lst_of_products_and_prices:
    product_name, product_price = item.rsplit(' - ', 1)
    product_dict[product_name] = float(product_price)


def get_products():
    products_list = []
    for item in lst_of_products_and_prices:
        product_name, product_price = item.rsplit(' - ', 1)
        product_dict = {'name': product_name, 'price': float(product_price)}
        # product_dict[product_name] = float(product_price)
        products_list.append(product_dict)
        
    return products_list