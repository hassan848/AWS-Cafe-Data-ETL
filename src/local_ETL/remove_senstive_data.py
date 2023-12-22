import csv

#removes the sensitve data ("name" and "card_number" from the dictionaries)                
def extract(Filename):
    with open(Filename,"r") as f:
        reader = csv.DictReader(f)
        data= list(reader)
        
        for row in data:
            del row["name"]
            del row["card_number"]
            
    return data

#To see it works as intended        
# print(extract("oneders\chesterfield_25-08-2021_09-00-00.csv"))
    

            
extraction = extract('chesterfield_25-08-2021_09-00-00.csv')
# for order in extraction:
#     print(order)

def save_data(filename, data):
    try:
        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
    except IndexError:
        print("An index error has occured. Will save an empty list to csv. ")
        return []
    except Exception as e:
        print(f"A {e} error has occured. Unable to save data to csv")
        
save_data('transformed_data.csv', extraction)