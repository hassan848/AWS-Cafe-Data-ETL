from remove_senstive_data import extract


def get_unique_branches():
    extraction = extract('chesterfield_25-08-2021_09-00-00.csv')
    branches = []
    branch_names = []
    for record in extraction:
        if record['location'] not in branch_names:
            branch_names.append(record['location'])
            branches.append({'branch_name': record['location']})
    return branches
        
# get_unique_branches()