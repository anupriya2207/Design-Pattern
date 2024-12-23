# Design-Pattern
import pandas as pd

# Check whether app clients ids are valid
def check_app_client_ids(file1, file2, file4, file5):
    # Load the CSV files into DataFrames
    df1 = pd.read_excel(file1)
    df2 = pd.read_csv(file2)

    # Extract app client ids from the respective columns
    app_client_ids_file1 = set(df1.iloc[:, 0].astype(str).str.strip())  # 1st column in file1
    app_client_ids_file2 = set(df2.iloc[:, 6].astype(str).str.strip())  # 7th column in file2 (index 6)

    # Find app client ids that are in file1 but not in file2
    missing_ids = app_client_ids_file1 - app_client_ids_file2

    # Print the results
    if not missing_ids:
        print("All app client ids in the Application_Inventory_Breakdown table are present in the thrd_prty_appln")
    else:
        print("The following app client ids are missing in the thrd_prty_appln table:")
        for missing_id in missing_ids:
            print(missing_id)

    # For Van data category check, get the valid app client ids, and then call the function that validates van data category
    valid_ids = app_client_ids_file1 - missing_ids
    for valid_id in valid_ids:
        print("Valid ids are:")
        print(valid_id)
    van_data_category_check(valid_ids, file4, file5)


# Check whether inventory classification identifier is valid
def check_inv_cls_ids(file1, file3):
	df1 = pd.read_excel(file1) 
	df2 = pd.read_excel(file3)
	# Extract inventory classification ids from the respective columns
	inv_cls_ids_file1 = set(df1.iloc[:, 1])  # 2nd column in file1
	inv_cls_ids_file3 = set(df2.iloc[:, 0])  # 1st column in file3 

	# Find inventory classification ids that are in file1 but not in file3
	missing_inv_cls_ids = inv_cls_ids_file1 - inv_cls_ids_file3

	# Print the results
	if not missing_inv_cls_ids:
    		print("All inventory classification ids in the Application_Inventory_Breakdown table are present in the Inventory_Classification_table")
	else:
    		print("The following inventory classification id are missing in the Inventory_Classification_table :")
    		for missing_inv_cls_id in missing_inv_cls_ids:
        		print(missing_inv_cls_id)

#Check whether that app client id is associated to van data category
def van_data_category_check(app_client_ids, file4, file5):
    # Load the first CSV file
    file4_df = pd.read_csv(file4)

    # Load the second CSV file
    file5_df = pd.read_csv(file5)

    # Ensure the App Client IDs in both list and CSV are compared in uppercase
    file4_df.iloc[:, 0] = file4_df.iloc[:, 0].str.upper()  # Assuming column 1 is App_Client_ID
    app_client_ids_upper = [id.upper() for id in app_client_ids]

    # Iterate through each App Client ID
    for app_client_id in app_client_ids_upper:
        # Filter rows for the current App Client ID
        filtered_df = file4_df[file4_df.iloc[:, 0] == app_client_id]

        # Check if there are rows for the current App Client ID
        if filtered_df.empty:
            print(f"No data found for App Client ID: {app_client_id}")
            continue

        # Group by the second column and get the latest version (third column)
        latest_versions = (
            filtered_df.groupby(filtered_df.iloc[:, 1], as_index=False)
            .apply(lambda group: group.loc[group.iloc[:, 2].idxmax()])
            .reset_index(drop=True)
        )

        # Map the second column of the first file to the second column of the second file
        mapped_results = pd.merge(
            latest_versions.iloc[:, [1]],  # Take only the second column (Data Category) from latest_versions
            file5_df.iloc[:, [1, 2]],  # Map with the second and third columns from file5_df
            left_on=latest_versions.columns[1],  # Column 2 from latest_versions
            right_on=file5_df.columns[1],  # Column 2 from file5_df
            how='inner'
        )

        # Add the App Client ID to the mapped results
        mapped_results['App_Client_ID'] = app_client_id

        # Check if data_clstr_nm is VAN_CAT
        if 'VIRTUAL_ACCT_NUM' in mapped_results.iloc[:, 1].values:  # Assuming the second column in mapped_results is data_clstr_nm
            print(f"VAN_CAT found for App Client ID: {app_client_id}")
        else:
            print(f"VAN_CAT not found for App Client ID: {app_client_id}")

        # Display the mapped result for the current App Client ID
        print(f"Mapped results for App Client ID: {app_client_id}")
        print(mapped_results)


# Validate whether  sum of percentage is equal to 100 for an app client id
def validate_percentage_sum(excel_file):
    # Load the Excel file into a DataFrame
    df_excel = pd.read_excel(excel_file)

    # Ensure columns are of correct types
    df_excel.iloc[:, 0] = df_excel.iloc[:, 0].astype(str).str.strip().str.upper()  # App client id (1st column) in uppercase
    df_excel.iloc[:, 1] = df_excel.iloc[:, 1].astype(int)                            # Inv_cls id (as integer)
    df_excel.iloc[:, 2] = df_excel.iloc[:, 2].astype(int)                            # Percentage column (as integer)

    # Get unique app_client_ids
    unique_app_clients = df_excel.iloc[:, 0].unique()

    # Iterate over each unique app_client_id
    for app_client in unique_app_clients:
        # Iterate over inv_cls_id starting with digits 1 to 9
        for start_digit in ['1', '2', '3', '4', '5', '6', '7', '8', '9']:
            # Filter data for the current app_client_id and inv_cls_id starting with the digit
            filtered_df = df_excel[(df_excel.iloc[:, 0] == app_client) & 
                                   (df_excel.iloc[:, 1].astype(str).str.startswith(start_digit))]

            if not filtered_df.empty:
                # Group by app_client_id and inv_cls_id, and sum the percentages for the filtered data
                grouped = filtered_df.groupby([df_excel.columns[0], df_excel.columns[1]])[df_excel.columns[2]].sum().reset_index()

                # Calculate the total sum of percentages for inv_cls_id starting with the current digit
                total_percentage_sum = grouped[df_excel.columns[2]].sum()

                # Check if the total sum of percentages is equal to 100
                if total_percentage_sum != 100:
                    print(f"\n** Invalid: The total sum of percentages for {app_client} (inv_cls_id starting with '{start_digit}') is {total_percentage_sum}. This should be 100.")
                    print(grouped)

#File paths for the input files
file1_path = r"C:\Users\n799731\PBB\Application_Inventory_Breakdown.xlsx"
file2_path = r"C:\Users\n799731\PBB\thrd_prty_appl.csv"
file3_path = r"C:\Users\n799731\PBB\Inventory_Classification_table_2.xlsx"
file4_path = r"C:\Users\n799731\PBB\thrd_prty_appl_data_cat.csv"
file5_path = r"C:\Users\n799731\PBB\data_cat.csv"

#Call the function
check_app_client_ids(file1_path, file2_path, file4_path, file5_path)
check_inv_cls_ids(file1_path, file3_path)
validate_percentage_sum(file1_path)





# Separate invalid rows
    invalid_rows = df1[df1.iloc[:, 0].astype(str).str.strip().isin(missing_ids)]

    # Save invalid rows to a new output file
    invalid_rows.to_excel(output_file_invalid, index=False)

    # Print the results
    if not missing_ids:
        print("All app client ids in the Application_Inventory_Breakdown table are present in the thrd_prty_appln")
    else:
        print("The following app client ids are missing in the thrd_prty_appln table and saved to a new file:")
        for missing_id in missing_ids:
            print(missing_id)

# Example usage
file1 = 'Application_Inventory_Breakdown.xlsx'  # Replace with the actual path to the first file
file2 = 'thrd_prty_appln.csv'  # Replace with the actual path to the second file
output_file_invalid = 'Invalid_App_Client_IDs.xlsx'  # File to save invalid rows

check_app_client_ids_and_filter(file1, file2, output_file_invalid)

