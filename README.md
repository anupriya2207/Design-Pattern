# Design-Pattern

# Load the CSV file
file_path = 'file4.csv'  # Replace with the actual path to file4
file4_df = pd.read_csv(file_path)

# List of App Client IDs to match
app_client_ids = ['ID1', 'ID2', 'ID3']  # Replace with your list of App Client IDs

# Ensure the App Client IDs in both list and CSV are compared in uppercase
file4_df['App_Client_ID'] = file4_df['App_Client_ID'].str.upper()
app_client_ids_upper = [id.upper() for id in app_client_ids]

# Initialize an empty list to collect results
results = []

# Iterate through each App Client ID
for app_client_id in app_client_ids_upper:
    # Filter rows for the current App Client ID
    filtered_df = file4_df[file4_df['App_Client_ID'] == app_client_id]
    
    # Group by Data_Cat and get the latest version
    latest_versions = (
        filtered_df.groupby('Data_Cat', as_index=False)
        .apply(lambda group: group.loc[group['Version'].idxmax()])
        .reset_index(drop=True)
    )
    
    # Append the results to the list
    results.append(latest_versions)

# Combine all results into a single DataFrame
final_df = pd.concat(results, ignore_index=True)

# Print the result
print("Filtered results:")
print(final_df)




# Ensure the App Client IDs in both list and CSV are compared in uppercase
file4_df.iloc[:, 0] = file4_df.iloc[:, 0].str.upper()  # Assuming column 1 is App_Client_ID
app_client_ids_upper = [id.upper() for id in app_client_ids]

# Initialize an empty list to collect results
results = []

# Iterate through each App Client ID
for app_client_id in app_client_ids_upper:
    # Filter rows for the current App Client ID
    filtered_df = file4_df[file4_df.iloc[:, 0] == app_client_id]
    
    # Group by the second column and get the latest version (third column)
    latest_versions = (
        filtered_df.groupby(filtered_df.iloc[:, 1], as_index=False)
        .apply(lambda group: group.loc[group.iloc[:, 2].idxmax()])
        .reset_index(drop=True)
    )
