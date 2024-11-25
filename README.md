# Design-Pattern
def sum_percentage_for_inv_cls_starting_with_1_to_9(excel_file):
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
