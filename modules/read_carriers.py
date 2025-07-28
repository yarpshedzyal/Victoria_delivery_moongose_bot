import pandas as pd

def get_proper_carriers_from_csv(file_path='modules\carriers.csv'):
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    # Form the carrier dictionary
    carrier_dict = {}
    for _, row in df.iterrows():
        name = row['name']
        variations = row.drop('name').dropna().values
        for variation in variations:
            carrier_dict[variation] = name
        carrier_dict[name] = name  # Add the name itself as a proper carrier
    
    return carrier_dict
