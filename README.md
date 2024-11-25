# Design-Pattern
def validate_percentage_sum(csv_file):
    # Load the CSV file into a DataFrame
    df_csv = pd.read_csv(csv_file)

    # Ensure columns are of correct types
    df_csv.iloc[:, 0] = df_csv.iloc[:, 0].astype(str).str.strip()  # App client id (1st column)
    df_csv.iloc[:, 1] = df_csv.iloc[:, 1].astype(str).str.strip()  # Inv_cls id (as string to check starting digit)
    df_csv.iloc[:, 2] = df_csv.iloc[:, 2].astype(float)            # Percentage column (3rd column)

    # Initialize a list to collect all invalid combinations
    all_invalid_combinations = []

    # Loop through the inv_cls id starting numbers to check
    for start_digit in range(1, 6):  # Covers 1, 2, 3, 4, 5
        # Filter rows where inv_cls id starts with the current digit
        filtered_df = df_csv[df_csv.iloc[:, 1].str.startswith(str(start_digit))]

        # Group by app client id and starting digit, summing percentages
        grouped = filtered_df.groupby([df_csv.columns[0], df_csv.columns[1]])[df_csv.columns[2]].sum()

        # Find invalid combinations where the sum != 100
        invalid_combinations = grouped[grouped != 100]

        # For each invalid combination, find the original rows contributing to it
        for (app_client, inv_cls) in invalid_combinations.index:
            invalid_rows = filtered_df[
                (filtered_df.iloc[:, 0] == app_client) & (filtered_df.iloc[:, 1] == inv_cls)
            ]
            all_invalid_combinations.append((app_client, inv_cls, invalid_rows, invalid_combinations[(app_client, inv_cls)]))

    # Output only invalid combinations with details (excluding inv_cls_group)
    if all_invalid_combinations:
        print("Invalid combinations (sum != 100) with contributing rows:")
        for app_client, inv_cls, rows, total in all_invalid_combinations:
            print(f"\nApp client id: {app_client}, Inv_cls id: {inv_cls}, Total: {total}")
            print(rows.to_string(index=False))
    else:
        print("All combinations satisfy the condition (sum = 100).")
