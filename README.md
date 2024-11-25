# Design-Pattern
def sum_percentage(excel_file):
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
        print(f"\nSumming percentages for app_client_id: {app_client} with inv_cls_id starting with 1")

        # Filter data for the current app_client_id and inv_cls_id starting with '1'
        filtered_df = df_excel[(df_excel.iloc[:, 0] == app_client) & (df_excel.iloc[:, 1].astype(str).str.startswith('1'))]

        # Group by app_client_id and inv_cls_id, and sum the percentages
        grouped = filtered_df.groupby([df_excel.columns[0], df_excel.columns[1]])[df_excel.columns[2]].sum().reset_index()

        # Print the grouped data and the sum of percentages for the current app_client_id
        print(grouped)

        # Calculate and print the total sum of percentages for all inv_cls_id starting with '1' for this app_client_id
        total_percentage_sum = grouped[df_excel.columns[2]].sum()
        print(f"Total Sum of Percentages for {app_client}: {total_percentage_sum}")
