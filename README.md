# Design-Pattern


import pandas as pd

def check_app_client_ids(file1, file2):
    # Load the CSV files into DataFrames
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Extract app client ids from the respective columns
    app_client_ids_file1 = set(df1.iloc[:, 0])  # 1st column in file1
    app_client_ids_file2 = set(df2.iloc[:, 6])  # 7th column in file2 (index 6)

    # Find app client ids that are in file1 but not in file2
    missing_ids = app_client_ids_file1 - app_client_ids_file2

    # Print the results
    if not missing_ids:
        print("All app client ids in the first CSV are present in the second CSV.")
    else:
        print("The following app client ids are missing in the second CSV:")
        for missing_id in missing_ids:
            print(missing_id)

# File paths for the input CSVs
file1_path = "file1.csv"
file2_path = "file2.csv"

# Call the function
check_app_client_ids(file1_path, file2_path)
