# Design-Pattern
def validate_percentage_sum(excel_file):
    # Load the Excel file into a DataFrame
    df_excel = pd.read_excel(excel_file)

    # Ensure columns are of correct types
    df_excel.iloc[:, 0] = df_excel.iloc[:, 0].astype(str).str.strip().str.upper()  # App client id (1st column) in uppercase
    df_excel.iloc[:, 1] = df_excel.iloc[:, 1].astype(int)                            # Inv_cls id (as integer)
    df_excel.iloc[:, 2] = df_excel.iloc[:, 2].astype(int)                            # Percentage column (as integer)

    # Initialize a list to collect all invalid combinations
    all_invalid_combinations = []

    # Loop through the inv_cls id starting numbers to check (1 to 5)
    for start_digit in range(1, 6):  # Covers 1, 2, 3, 4, 5
        # Filter rows where inv_cls id starts with the current digit
        filtered_df = df_excel[df_excel.iloc[:, 1].astype(str).str.startswith(str(start_digit))]

        # Group by app_client_id and inv_cls_id, and sum the percentages
        grouped = filtered_df.groupby([df_excel.columns[0], df_excel.columns[1]])[df_excel.columns[2]].sum().reset_index()

        # Check if the sum of percentages is 100 for each (app_client_id, inv_cls_id) combination
        for _, row in grouped.iterrows():
            app_client = row[df_excel.columns[0]]
            inv_cls = row[df_excel.columns[1]]
            percentage_sum = row[df_excel.columns[2]]

            if percentage_sum != 100:
                # If the sum is not 100, add the rows contributing to the invalid sum
                invalid_rows = filtered_df[
                    (filtered_df.iloc[:, 0] == app_client) & (filtered_df.iloc[:, 1] == inv_cls)
                ]
                all_invalid_combinations.append((app_client, inv_cls, invalid_rows, percentage_sum))

    # Output only invalid combinations with details
    if all_invalid_combinations:
        print("Invalid combinations (sum != 100) with contributing rows:")
        for app_client, inv_cls, rows, total in all_invalid_combinations:
            print(f"\nApp client id: {app_client}, Inv_cls id: {inv_cls}, Total: {total}")
            print(rows.to_string(index=False))
    else:
        print("All combinations satisfy the condition (sum = 100).")
