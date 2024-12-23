# Design-Pattern


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

    # Display the mapped result for the current App Client ID
    print(f"Mapped results for App Client ID: {app_client_id}")
    print(mapped_results)


    # Check if data_clstr_nm is VAN_CAT
    if 'VAN_CAT' in mapped_results.iloc[:, 1].values:  # Assuming the second column in mapped_results is data_clstr_nm
        print(f"VAN_CAT found for App Client ID: {app_client_id}")
    else:
        print(f"VAN_CAT not found for App Client ID: {app_client_id}")
