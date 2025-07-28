# import pandas as pd

# # df = pd.read_csv('income/test1.csv')

def classify_order_id(df):
    class_thestore = set()
    class_webstore = set()

    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        order_id = row['order_id']
        
        # Example classification condition (e.g., even or odd)
        if len(str(order_id)) == 10:  # Adjust this condition based on your criteria
            class_thestore.add(order_id)
        else:
            class_webstore.add(order_id)

    return class_thestore, class_webstore



