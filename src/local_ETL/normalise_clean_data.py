from remove_senstive_data import extract

clean_data = extract('chesterfield_25-08-2021_09-00-00.csv')

def get_normalised_transactions():
    for record in clean_data:
        record['basket'] = record['basket'].split(', ')
        items = []
        for item in record['basket']:
            quantity = record['basket'].count(item)
            item += f' - {quantity}'
            if quantity > 1 and item in items: continue
            items.append(item)
            
        record['basket'] = items
        
        for i in range(len(record['basket'])):
            product_name, product_price, quantity = record['basket'][i].rsplit(' - ', 2)
                
            product_dict = {
                'product' : product_name,
                'price' : float(product_price),
                'quantity' : int(quantity)
            }
            record['basket'][i] = product_dict
            
    return clean_data

# trans = get_normalised_transactions()
# for i in range(len(trans)):
#     if i == 28:
#         print(trans[i])
# for product, i in enumerate(trans[28]['basket']):
#     print(product, i)